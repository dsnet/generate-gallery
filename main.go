// Copyright 2021, Joe Tsai. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE.md file.

package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"errors"
	"flag"
	"fmt"
	"html"
	"image"
	"image/draw"
	"image/jpeg"
	"image/png"
	"io"
	"log"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/disintegration/imaging"
	"github.com/rwcarlsen/goexif/exif"
)

const (
	defaultHeight = 160
	defaultSortBy = "creation_date"
)

var (
	height  = flag.Int("height", 0, "Pixel height of each thumbnail. (default: "+strconv.Itoa(defaultHeight)+")")
	sortby  = flag.String("sortby", "", "Sort the gallery according 'creation_date' or 'file_path'. (default: \"creation_date\")")
	exclude = flag.String("exclude", "", "Regular expression pattern of paths to exclude. (default: none)")
	procs   = flag.Int("procs", runtime.NumCPU(), "Number of concurrent workers.")
)

func main() {
	// Process command line flags.
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), strings.Join([]string{
			"Usage: %s [OPTION]... DIR",
			"",
			"This generates a static HTML file at DIR.html containing previews",
			"of all the images and videos in the specified directory.",
			"If DIR.html already exists, it is parsed and the original parameters",
			"and any up-to-date preview items will be used for regeneration.",
			"Otherwise, the generation parameters used are the defaults listed below.",
			"",
			"",
		}, "\n"), os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()
	if flag.NArg() != 1 {
		fmt.Fprintf(flag.CommandLine.Output(), "Directory to generate gallery from not specified.\n\n")
		flag.Usage()
		os.Exit(1)
	}

	// Change into the parent directory.
	dir := filepath.Clean(flag.Arg(0))
	dirName := filepath.Base(dir)
	if err := os.Chdir(filepath.Dir(dir)); err != nil {
		log.Fatalf("os.Chdir error: %v", err)
	}
	htmlFile := dirName + ".html"

	// Parse existing .html gallery (if existing).
	var page galleryPage
	var cachedItems map[string]mediaItem
	if b, err := os.ReadFile(htmlFile); err == nil {
		log.Printf("parsing existing %v", htmlFile)

		page, err = unmarshalPage(b)
		if err != nil {
			log.Fatalf("unmarshalPage error: %v", err)
		}

		// Instead of directly using the previous items,
		// use them as a cache in case files have been deleted or modified.
		cachedItems = make(map[string]mediaItem)
		for _, item := range page.items {
			cachedItems[item.filepath] = item
		}
		page.items = nil

		// If the preview height for the previous gallery differs from
		// the specified height, then the previous entries are useless.
		if *height != 0 && *height != page.Height {
			log.Printf("discarding cached items since preview height changed: %d => %d", page.Height, *height)
			cachedItems = nil
		}
	}

	// Handle gallery generation parameters.
	var flags []string
	var excludeRx *regexp.Regexp
	var sema chan struct{}
	if *height != 0 {
		page.Height = *height
	} else if page.Height == 0 {
		page.Height = defaultHeight
	}
	if page.Height <= 0 {
		fmt.Fprintf(flag.CommandLine.Output(), "Invalid 'height' value: %v\n\n", page.Height)
		flag.Usage()
		os.Exit(1)
	}
	flags = append(flags, fmt.Sprintf("\t-height=%d", page.Height))
	if *sortby != "" {
		page.SortBy = *sortby
	} else if page.SortBy == "" {
		page.SortBy = defaultSortBy
	}
	if page.SortBy != "creation_date" && page.SortBy != "file_path" {
		fmt.Fprintf(flag.CommandLine.Output(), "Invalid 'sortby' value: %v\n\n", page.SortBy)
		flag.Usage()
		os.Exit(1)
	}
	flags = append(flags, fmt.Sprintf("\t-sortby=%s", page.SortBy))
	if *exclude != "" {
		page.Exclude = *exclude
	}
	if page.Exclude != "" {
		var err error
		excludeRx, err = regexp.Compile(page.Exclude)
		if err != nil {
			fmt.Fprintf(flag.CommandLine.Output(), "Invalid 'exclude' pattern: %v\n\n", page.Exclude)
			flag.Usage()
			os.Exit(1)
		}
		flags = append(flags, fmt.Sprintf("\t-exclude=%s", page.Exclude))
	}
	if *procs <= 0 {
		*procs = runtime.NumCPU()
	}
	sema = make(chan struct{}, *procs)
	log.Printf("generation flags:\n%s", strings.Join(flags, "\n"))

	// Collect all files in the directory.
	allFileExts := make(map[string][]string)
	allFileInfos := make(map[string]os.FileInfo)
	if err := filepath.Walk(dirName, func(fp string, fi os.FileInfo, err error) error {
		if err != nil || fi.IsDir() {
			return err
		}
		ext := path.Ext(fp)
		if imageFormatFromExt(ext) != invalidFormat {
			name := strings.TrimSuffix(fp, ext)
			allFileExts[name] = append(allFileExts[name], ext)
			allFileInfos[fp] = fi
		}
		return nil
	}); err != nil {
		log.Fatalf("filepath.Walk error: %v", err)
	}

	// Collect up all the media items in the gallery.
	for name, exts := range allFileExts {
		if len(exts) > 1 {
			// Multiple extensions exist. Sort them such that static images
			// take precedence over animated media.
			sort.Slice(exts, func(i, j int) bool {
				fi := imageFormatFromExt(exts[i])
				fj := imageFormatFromExt(exts[j])
				if fi != fj {
					return fi < fj
				}
				return exts[i] < exts[j]
			})
		}
		fp := name + exts[0]
		fi := allFileInfos[fp]
		if excludeRx != nil && excludeRx.MatchString("/"+filepath.ToSlash(fp)) {
			continue
		}
		page.items = append(page.items, mediaItem{
			filepath: filepath.ToSlash(fp),
			mediaMetadata: mediaMetadata{
				FileSize:   fi.Size(),
				FileModify: fi.ModTime().UTC(),
			},
		})
	}
	sort.Slice(page.items, func(i, j int) bool {
		return page.items[i].filepath < page.items[j].filepath
	})
	log.Printf("processing %d items", len(page.items))

	// Process every media item.
	var wg sync.WaitGroup
	var numCached int
	lastPrint := time.Now()
	for i := range page.items {
		// Print progress.
		if now := time.Now(); now.Sub(lastPrint) > time.Second {
			log.Printf("%d items processed (%0.3f%%)", i, 100.0*float64(i)/float64(len(page.items)))
			lastPrint = now
		}

		// Check cache for item.
		item := &page.items[i]
		if cachedItem, ok := cachedItems[item.filepath]; ok &&
			item.FileSize == cachedItem.FileSize &&
			item.FileModify.Equal(cachedItem.FileModify) {
			*item = cachedItem
			numCached++
			continue
		}

		// Process each item.
		sema <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sema }()
			if err := item.loadMetadata(); err != nil {
				log.Printf("%s: loadMetadata error: %v", item.filepath, err)
			}
			if err := item.computePreview(page.Height); err != nil {
				log.Printf("%s: computePreview error: %v", item.filepath, err)
			}
		}()
	}
	wg.Wait()
	log.Printf("%d items processed (%d from cache)", len(page.items), numCached)

	// Sort the items.
	if page.SortBy == "creation_date" {
		sort.Slice(page.items, func(i, j int) bool {
			ti := page.items[i].dateTime()
			tj := page.items[j].dateTime()
			if !ti.Equal(tj) {
				return ti.Before(tj)
			}
			return page.items[i].filepath < page.items[j].filepath
		})
	}

	// Write the gallery HTML.
	html, err := marshalPage(page)
	if err != nil {
		log.Fatalf("marshalPage error: %v", err)
	}
	if b, err := os.ReadFile(htmlFile); err == nil && bytes.Equal(b, html) {
		log.Printf("no changes made to %v", htmlFile)
		return // skip writing the file if identical
	}
	if err := os.WriteFile(htmlFile, html, 0664); err != nil {
		log.Fatalf("os.WriteFile error: %v", err)
	}
	log.Printf("wrote %v", htmlFile)
}

func unmarshalPage(b []byte) (galleryPage, error) {
	var page galleryPage
	var parsedHeader int
	lines := strings.Split(string(b), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(line, "<html") && strings.HasSuffix(line, ">"):
			parsedHeader++
			var html struct {
				XMLName  xml.Name `xml:"html"`
				Magic    string   `xml:"data-magic,attr"`
				Metadata string   `xml:"data-gallery,attr"`
			}
			if err := xml.Unmarshal([]byte(line+"</html>"), &html); err != nil {
				return page, err
			}
			if html.Magic != "generate-gallery" {
				return page, errors.New("missing magic marker")
			}
			b, err := base64.StdEncoding.DecodeString(html.Metadata)
			if err != nil {
				return page, err
			}
			if err := json.Unmarshal(b, &page.galleryMetadata); err != nil {
				return page, err
			}
		case strings.HasPrefix(line, "<a ") && strings.HasSuffix(line, "</a>"):
			var item mediaItem
			var anchor struct {
				XMLName   xml.Name `xml:"a"`
				Reference string   `xml:"href,attr"`
				Image     struct {
					XMLName  xml.Name `xml:"img"`
					Source   string   `xml:"src,attr"`
					Metadata string   `xml:"data-media,attr"`
				}
			}
			if err := xml.Unmarshal([]byte(line), &anchor); err != nil {
				return page, err
			}
			u, err := url.Parse(anchor.Reference)
			if err != nil {
				return page, err
			}
			item.filepath = u.Path
			item.previewSrc = anchor.Image.Source
			b, err := base64.StdEncoding.DecodeString(anchor.Image.Metadata)
			if err != nil {
				return page, err
			}
			if err := json.Unmarshal(b, &item.mediaMetadata); err != nil {
				return page, err
			}
			page.items = append(page.items, item)
		}
	}
	switch {
	case parsedHeader < 1:
		return page, errors.New("html tag missing")
	case parsedHeader > 1:
		return page, errors.New("html tag appeared multiple times")
	}
	return page, nil
}

func marshalPage(page galleryPage) ([]byte, error) {
	var bb bytes.Buffer
	b, err := json.Marshal(page.galleryMetadata)
	if err != nil {
		return nil, err
	}
	metadata := ` data-gallery="` + base64.StdEncoding.EncodeToString(b) + `"`
	bb.WriteString("<html data-magic=\"generate-gallery\"" + metadata + ">\n")
	bb.WriteString("<body>\n")
	for _, item := range page.items {
		if len(item.previewSrc) > 0 {
			title := ` title="` + html.EscapeString(path.Base(item.filepath)) + "; " + item.dateTime().UTC().Round(time.Second).Format("2006-01-02 15:04:05") + `"`
			b, err := json.Marshal(item.mediaMetadata)
			if err != nil {
				return nil, err
			}
			metadata := ` data-media="` + base64.StdEncoding.EncodeToString(b) + `"`
			u := (&url.URL{Path: item.filepath}).String()
			u = html.EscapeString(u)
			bb.WriteString("<a href=\"" + u + "\" target=\"_blank\"><img src=\"" + item.previewSrc + "\"" + title + metadata + "/></a>\n")
		}
	}
	bb.WriteString("</body>\n")
	bb.WriteString("</html>\n")
	return bb.Bytes(), nil
}

type imageFormat int

const (
	invalidFormat imageFormat = iota
	jpgFormat
	pngFormat
	gifFormat
	webpFormat
	webmFormat
	mp4Format
)

func imageFormatFromExt(ext string) imageFormat {
	switch {
	case strings.EqualFold(ext, ".jpg") || strings.EqualFold(ext, ".jpeg"):
		return jpgFormat
	case strings.EqualFold(ext, ".png"):
		return pngFormat
	case strings.EqualFold(ext, ".gif"):
		return gifFormat
	case strings.EqualFold(ext, ".webp"):
		return webpFormat
	case strings.EqualFold(ext, ".webm"):
		return webmFormat
	case strings.EqualFold(ext, ".mp4"):
		return mp4Format
	default:
		return invalidFormat
	}
}

type galleryPage struct {
	// galleryMetadata is metadata about the gallery.
	galleryMetadata
	// items is the list of media items in the gallery.
	items []mediaItem
}

type galleryMetadata struct {
	// Height is the pixel height of the preview image.
	Height int
	// SortBy is the order to sort preview images by.
	SortBy string
	// Exclude is the regular expression pattern of paths to exclude.
	Exclude string `json:",omitempty"`
}

// mediaItem is an individual thumbnail to show on the gallery page.
type mediaItem struct {
	// filepath is the relative file path using forward slashes.
	filepath string // e.g., "2021Q1/IMG_6189.JPG"
	// mediaMetadata is metadata about the file and/or media.
	mediaMetadata
	// orientImage modifies an image according to orientation metadata.
	orientImage func(image.Image) image.Image
	// previewSrc is a preview image source for the media item.
	previewSrc string // e.g., "data:image/jpeg;base64, {{.Base64EncodedData}}>"
}

// mediaMetadata is metadata regarding a single media item.
// The exported fields are serialized as metadata in the .html file.
type mediaMetadata struct {
	// FileSize is the fs.FileInfo.Size for the file on disk.
	FileSize int64
	// FileModify is the fs.FileInfo.ModTime for the file on disk.
	FileModify time.Time
	// MediaCreate is the creation time according to the file metadata.
	MediaCreate time.Time
}

// dateTime returns the media creation timestamp if available,
// otherwise it returns the file modify timestamp.
func (item mediaItem) dateTime() time.Time {
	if !item.MediaCreate.IsZero() {
		return item.MediaCreate
	}
	return item.FileModify
}

// loadMetadata loads media-specific metadata from EXIF or XMP.
// It populates item.MediaCreate and item.orientImage.
func (item *mediaItem) loadMetadata() error {
	fp := filepath.FromSlash(item.filepath)
	ext := filepath.Ext(fp)
	switch imageFormatFromExt(ext) {
	case jpgFormat:
		// Read the EXIF metadata in the image.
		f, err := os.Open(fp)
		if err != nil {
			return err
		}
		defer f.Close()
		x, err := exif.Decode(f)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		// Handle EXIF creation/modify timestamps.
		t, err := x.DateTime()
		if err != nil && !exif.IsTagNotPresentError(err) {
			return err
		}
		if err == nil && !t.IsZero() {
			item.MediaCreate = t.UTC()
		}

		// Handle EXIF orientation data.
		orient, err := x.Get(exif.Orientation)
		if err != nil && !exif.IsTagNotPresentError(err) {
			return err
		}
		if err == nil && orient != nil {
			switch orient.String() {
			case "1":
				item.orientImage = func(img image.Image) image.Image { return img }
			case "2":
				item.orientImage = func(img image.Image) image.Image { return imaging.FlipH(img) }
			case "3":
				item.orientImage = func(img image.Image) image.Image { return imaging.Rotate180(img) }
			case "4":
				item.orientImage = func(img image.Image) image.Image { return imaging.Rotate180(imaging.FlipH(img)) }
			case "5":
				item.orientImage = func(img image.Image) image.Image { return imaging.Rotate270(imaging.FlipV(img)) }
			case "6":
				item.orientImage = func(img image.Image) image.Image { return imaging.Rotate270(img) }
			case "7":
				item.orientImage = func(img image.Image) image.Image { return imaging.Rotate90(imaging.FlipV(img)) }
			case "8":
				item.orientImage = func(img image.Image) image.Image { return imaging.Rotate90(img) }
			}
		}
	case webmFormat, mp4Format:
		// Treat .JSON files as the ffprobe output for the movie file.
		out, err := os.ReadFile(strings.TrimSuffix(fp, ext) + ".JSON")
		if err != nil {
			out, err = os.ReadFile(strings.TrimSuffix(fp, ext) + ".json")
			if err != nil {
				// Otherwise, try to read the movie metadata using ffprobe.
				out, err = exec.Command("ffprobe", "-v", "quiet", fp, "-print_format", "json", "-show_format").Output()
				if err != nil {
					return fmt.Errorf("ffprobe error: %v", err)
				}
			}
		}

		// Parse the ffprobe JSON output for the creation time.
		var v struct {
			Format struct {
				Tags struct {
					CreationTime time.Time `json:"creation_time"`
				} `json:"tags"`
			} `json:"format"`
		}
		if err := json.Unmarshal(out, &v); err != nil {
			return err
		}
		if t := v.Format.Tags.CreationTime; !t.IsZero() {
			item.MediaCreate = t.UTC()
		}
	}
	return nil
}

// computePreview generates a preview image for the media item.
// It populates item.previewSrc.
func (item *mediaItem) computePreview(height int) error {
	fp := filepath.FromSlash(item.filepath)
	switch format := imageFormatFromExt(filepath.Ext(fp)); format {
	case jpgFormat, pngFormat:
		// Read and decode the image.
		b, err := os.ReadFile(fp)
		if err != nil {
			return err
		}
		img, _, err := image.Decode(bytes.NewReader(b))
		if err != nil {
			return err
		}

		// Resize the image.
		if item.orientImage != nil {
			img = item.orientImage(img)
		}
		img = resizeImage(img, height)

		// Encode and write the image.
		var bb bytes.Buffer
		if opaque, ok := img.(interface{ Opaque() bool }); ok && opaque.Opaque() {
			if err := jpeg.Encode(&bb, img, nil); err != nil {
				return err
			}
			item.previewSrc = "data:image/jpeg;base64, " + base64.StdEncoding.EncodeToString(bb.Bytes())
		} else {
			if err := png.Encode(&bb, img); err != nil {
				return err
			}
			item.previewSrc = "data:image/png;base64, " + base64.StdEncoding.EncodeToString(bb.Bytes())
		}

	case gifFormat, webpFormat:
		tmp1, err := os.MkdirTemp("", "generate-gallery")
		if err != nil {
			return err
		}
		defer os.RemoveAll(tmp1)
		tmp2, err := os.MkdirTemp("", "generate-gallery")
		if err != nil {
			return err
		}
		defer os.RemoveAll(tmp2)

		// Convert the animated image into a series of frames.
		if out, err := exec.Command("ffmpeg", "-i", fp, filepath.Join(tmp1, "frame_%08d.png")).CombinedOutput(); err != nil {
			if format == webpFormat {
				// TODO: As of 2021-07-04, ffmpeg cannot decode WebP images.
				// See https://trac.ffmpeg.org/ticket/4907.
				return fmt.Errorf("not supported")
			}
			return fmt.Errorf("ffmpeg decode error: %v\n%v", err, indent(string(out)))
		}

		// Count the total number of frames.
		var totalFrames int
		fis, err := os.ReadDir(tmp1)
		if err != nil {
			return err
		}
		for _, fi := range fis {
			if !fi.IsDir() && path.Ext(fi.Name()) == ".png" {
				totalFrames++
			}
		}

		// Periodically sample several of the frames.
		var numFrames int
		switch {
		case totalFrames <= 1:
			numFrames = 1
		case totalFrames <= 16:
			numFrames = 2
		case totalFrames <= 256:
			numFrames = 4
		default:
			numFrames = 8
		}
		framePeriod := totalFrames / numFrames

		// Decode, resize, and format each frame.
		var bb bytes.Buffer
		for i, j := 0, 0; i < totalFrames; i, j = i+framePeriod, j+1 {
			// Read and decode the frame.
			b, err := os.ReadFile(filepath.Join(tmp1, fmt.Sprintf("frame_%08d.png", i+1)))
			if err != nil {
				return err
			}
			img, err := png.Decode(bytes.NewReader(b))
			if err != nil {
				return err
			}

			// Resize the image.
			img = resizeImage(img, height)

			// Encode and write the frame.
			bb.Reset()
			if err := png.Encode(&bb, img); err != nil {
				return err
			}
			if err := os.WriteFile(filepath.Join(tmp2, fmt.Sprintf("frame_%04d.png", j+1)), bb.Bytes(), 0664); err != nil {
				return err
			}
		}

		// Format the frames as an animated WebP preview.
		out, err := exec.Command("ffmpeg", "-r", "4", "-i", filepath.Join(tmp2, "frame_%04d.png"), "-loop", "0", filepath.Join(tmp2, "preview.webp")).CombinedOutput()
		if err != nil {
			return fmt.Errorf("ffmpeg encode error: %v\n%v", err, indent(string(out)))
		}
		out, err = os.ReadFile(filepath.Join(tmp2, "preview.webp"))
		if err != nil {
			return err
		}
		item.previewSrc = "data:image/webp;base64, " + base64.StdEncoding.EncodeToString(out)

	case webmFormat, mp4Format:
		tmp, err := os.MkdirTemp("", "generate-gallery")
		if err != nil {
			return err
		}
		defer os.RemoveAll(tmp)

		// Retrieve the video duration.
		out, err := exec.Command("ffprobe", "-i", fp, "-show_entries", "format=duration", "-v", "quiet", "-of", `csv=p=0`).Output()
		if err != nil {
			return fmt.Errorf("ffprobe error: %v", err)
		}
		duration := strings.TrimSpace(string(out))
		dur, err := strconv.ParseFloat(duration, 64)
		if err != nil {
			return err
		}

		// Periodically sample several of the frames.
		if dur < 10.0 {
			// For short videos, produce individual frames in a single pass.
			frames := 8
			if dur < 5.0 {
				frames = 4
			}
			if out, err = exec.Command("ffmpeg", "-i", fp, "-vf", "scale=-1:"+strconv.Itoa(height)+",fps="+strconv.Itoa(frames)+"/"+duration, filepath.Join(tmp, "frame_%04d.jpeg")).CombinedOutput(); err != nil {
				return fmt.Errorf("ffmpeg decode error: %v\n%v", err, indent(string(out)))
			}
		} else {
			// For long videos, produce individual frames by seeking.
			for i := 1; i <= 10; i++ {
				seek := fmt.Sprintf("%f", dur*float64(i)/float64(11))
				if out, err = exec.Command("ffmpeg", "-ss", seek, "-i", fp, "-vf", "scale=-1:"+strconv.Itoa(height), "-vframes", "1", filepath.Join(tmp, fmt.Sprintf("frame_%04d.jpeg", i))).CombinedOutput(); err != nil {
					return fmt.Errorf("ffmpeg decode error: %v\n%v", err, indent(string(out)))
				}
			}
		}

		// Format the frames as an animated WebP preview.
		out, err = exec.Command("ffmpeg", "-r", "2", "-i", filepath.Join(tmp, "frame_%04d.jpeg"), "-loop", "0", filepath.Join(tmp, "preview.webp")).CombinedOutput()
		if err != nil {
			return fmt.Errorf("ffmpeg encode error: %v\n%v", err, indent(string(out)))
		}
		out, err = os.ReadFile(filepath.Join(tmp, "preview.webp"))
		if err != nil {
			return err
		}
		item.previewSrc = "data:image/webp;base64, " + base64.StdEncoding.EncodeToString(out)
	}
	return nil
}

// resizeImage resizes the provided image to have the specified height.
// If the image height is smaller than the specified height,
// then it is extended, while keeping the image centered.
// If the image height is larger than the specified height,
// then the entire image is scaled down, while keeping the aspect ratio.
func resizeImage(src image.Image, height int) image.Image {
	dx, dy := src.Bounds().Dx(), src.Bounds().Dy()
	switch {
	case dy < height:
		dst := image.NewNRGBA(image.Rect(0, 0, dx, height))
		draw.Draw(dst, image.Rect(0, (height-dy)/2, dx, height), src, image.Pt(0, 0), draw.Over)
		return dst
	case dy > height:
		return imaging.Resize(src, 0, height, imaging.CatmullRom)
	default:
		return src
	}
}

func indent(in string) string {
	return strings.TrimRight("\t"+strings.Join(strings.Split(in, "\n"), "\n\t"), "\t")
}
