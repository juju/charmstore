// Copyright 2014 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package v4

import (
	"archive/zip"
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"

	"github.com/juju/jujusvg"
	"gopkg.in/errgo.v1"
	"gopkg.in/juju/charm.v4"

	"github.com/juju/charmstore/internal/mongodoc"
	"github.com/juju/charmstore/internal/router"
	"github.com/juju/charmstore/params"
)

// GET id/diagram.svg
// http://tinyurl.com/nqjvxov
func (h *Handler) serveDiagram(id *charm.Reference, fullySpecified bool, w http.ResponseWriter, req *http.Request) error {
	if id.Series != "bundle" {
		return errgo.WithCausef(nil, params.ErrNotFound, "diagrams not supported for charms")
	}
	entity, err := h.store.FindEntity(id, "bundledata")
	if err != nil {
		return errgo.Mask(err, errgo.Is(params.ErrNotFound))
	}

	var urlErr error
	// TODO consider what happens when a charm's SVG does not exist.
	canvas, err := jujusvg.NewFromBundle(entity.BundleData, func(id *charm.Reference) string {
		// TODO change jujusvg so that the iconURL function can
		// return an error.
		absPath := "/" + id.Path() + "/icon.svg"
		p, err := router.RelativeURLPath(req.RequestURI, absPath)
		if err != nil {
			urlErr = errgo.Notef(err, "cannot make relative URL from %q and %q", req.RequestURI, absPath)
		}
		return p
	})
	if err != nil {
		return errgo.Notef(err, "cannot create canvas")
	}
	if urlErr != nil {
		return urlErr
	}
	setArchiveCacheControl(w.Header(), fullySpecified)
	w.Header().Set("Content-Type", "image/svg+xml")
	canvas.Marshal(w)
	return nil
}

// These are all forms of README files
// actually observed in charms in the wild.
var allowedReadMe = map[string]bool{
	"readme":          true,
	"readme.md":       true,
	"readme.rst":      true,
	"readme.ex":       true,
	"readme.markdown": true,
	"readme.txt":      true,
}

// GET id/readme
// http://tinyurl.com/kygyvot
func (h *Handler) serveReadMe(id *charm.Reference, fullySpecified bool, w http.ResponseWriter, req *http.Request) error {
	entity, err := h.store.FindEntity(id, "_id", "contents", "blobname")
	if err != nil {
		return errgo.NoteMask(err, "cannot get README", errgo.Is(params.ErrNotFound))
	}
	isReadMeFile := func(f *zip.File) bool {
		name := strings.ToLower(path.Clean(f.Name))
		// This is the same condition currently used by the GUI.
		// TODO propagate likely content type from file extension.
		return allowedReadMe[name]
	}
	r, err := h.store.OpenCachedBlobFile(entity, mongodoc.FileReadMe, isReadMeFile)
	if err != nil {
		return errgo.Mask(err, errgo.Is(params.ErrNotFound))
	}
	defer r.Close()
	setArchiveCacheControl(w.Header(), fullySpecified)
	io.Copy(w, r)
	return nil
}

// GET id/icon.svg
// http://tinyurl.com/lhodocb
func (h *Handler) serveIcon(id *charm.Reference, fullySpecified bool, w http.ResponseWriter, req *http.Request) error {
	if id.Series == "bundle" {
		return errgo.WithCausef(nil, params.ErrNotFound, "icons not supported for bundles")
	}

	entity, err := h.store.FindEntity(id, "_id", "contents", "blobname")
	if err != nil {
		return errgo.NoteMask(err, "cannot get icon", errgo.Is(params.ErrNotFound))
	}
	isIconFile := func(f *zip.File) bool {
		return path.Clean(f.Name) == "icon.svg"
	}
	r, err := h.store.OpenCachedBlobFile(entity, mongodoc.FileIcon, isIconFile)
	if err != nil {
		if errgo.Cause(err) != params.ErrNotFound {
			return errgo.Mask(err)
		}
		setArchiveCacheControl(w.Header(), fullySpecified)
		w.Header().Set("Content-Type", "image/svg+xml")
		io.Copy(w, strings.NewReader(defaultIcon))
		return nil
	}
	defer r.Close()
	w.Header().Set("Content-Type", "image/svg+xml")
	setArchiveCacheControl(w.Header(), fullySpecified)
	return processIcon(w, r)
}

func processIcon(w io.Writer, r io.Reader) error {
	var saved bytes.Buffer
	dec := xml.NewDecoder(io.TeeReader(r, &saved))
	found, changed := false, false
	for !found {
		tok, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read token: %v", err)
		}
		_, found, changed = ensureViewbox(tok)
	}
	if !found {
		return fmt.Errorf("no svg token found")
	}
	// Stitch the input back together again.
	r = io.MultiReader(&saved, r)
	if !changed {
		_, err := io.Copy(w, r)
		return err
	}
	return processNaive(w, r)
}

func processNaive(w io.Writer, r io.Reader) error {
	dec := xml.NewDecoder(r)
	enc := xml.NewEncoder(w)
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read token: %v", err)
		}
		switch tok.(type) {
		// Simplify processing instructions
		case xml.ProcInst:
			w.Write([]byte(`<?xml version="1.0"?>`))
			continue
			// Wipe extraneous character data
		case xml.CharData:
			continue
		}
		tok, found, _ := ensureViewbox(tok)
		if found {
			writeRootToken(tok.(xml.StartElement), w)
		} else {
			if err := enc.EncodeToken(tok); err != nil {
				return fmt.Errorf("cannot encode token %#v: %v", tok, err)
			}
		}
	}
	if err := enc.Flush(); err != nil {
		return fmt.Errorf("cannot flush output: %v", err)
	}
	return nil
}

func ensureViewbox(tok0 xml.Token) (_ xml.Token, found, changed bool) {
	tok, ok := tok0.(xml.StartElement)
	if !ok || strings.ToLower(tok.Name.Local) != "svg" {
		return tok0, false, false
	}
	var width, height string
	for _, attr := range tok.Attr {
		if attr.Name.Space == "_xmlns" {
			attr.Name.Space = "xmlns"
		}
		switch strings.ToLower(attr.Name.Local) {
		case "width":
			width = attr.Value
		case "height":
			height = attr.Value
		case "viewbox":
			return tok, true, false
		}
	}
	tok.Attr = append(tok.Attr, xml.Attr{
		Name: xml.Name{
			Space: "",
			Local: "viewBox",
		},
		Value: fmt.Sprintf("0 0 %s %s", width, height),
	})
	return tok, true, true
}

func writeRootToken(tok xml.StartElement, w io.Writer) {
	namespaces := map[string]string{
		"xmlns": "xmlns",
	}
	w.Write([]byte(fmt.Sprintf("<%s", tok.Name.Local)))
	for _, attr := range tok.Attr {
		if attr.Name.Space == "xmlns" {
			namespaces[attr.Value] = attr.Name.Local
			w.Write([]byte(fmt.Sprintf(` xmlns:%s="%s"`, attr.Name.Local, attr.Value)))
		} else if attr.Name.Space != "" {
			w.Write([]byte(fmt.Sprintf(` %s:%s="%s"`, namespaces[attr.Name.Space], attr.Name.Local, attr.Value)))
		} else {
			w.Write([]byte(fmt.Sprintf(` %s="%s"`, attr.Name.Local, attr.Value)))
		}
	}
	w.Write([]byte(">"))
}
