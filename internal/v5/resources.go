// Copyright 2016 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package v5 // import "gopkg.in/juju/charmstore.v5-unstable/internal/v5"

import (
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"

	"github.com/juju/httprequest"
	"gopkg.in/errgo.v1"
	"gopkg.in/juju/charm.v6-unstable/resource"
	"gopkg.in/juju/charmrepo.v2-unstable/csclient/params"

	"gopkg.in/juju/charmstore.v5-unstable/internal/charmstore"
	"gopkg.in/juju/charmstore.v5-unstable/internal/mongodoc"
	"gopkg.in/juju/charmstore.v5-unstable/internal/router"
)

// GET id/meta/resource
// https://github.com/juju/charmstore/blob/v5/docs/API.md#get-idmetaresources
func (h *ReqHandler) metaResources(entity *mongodoc.Entity, id *router.ResolvedURL, path string, flags url.Values, req *http.Request) (interface{}, error) {
	if entity.URL.Series == "bundle" {
		return nil, nil
	}
	// TODO(ericsnow) Handle flags.
	// TODO(ericsnow) Use h.Store.ListResources() once that exists.
	resources, err := basicListResources(entity)
	if err != nil {
		return nil, err
	}
	results := make([]params.Resource, 0, len(resources))
	for _, res := range resources {
		result := params.Resource2API(res)
		results = append(results, result)
	}
	return results, nil
}

func basicListResources(entity *mongodoc.Entity) ([]resource.Resource, error) {
	if entity.CharmMeta == nil {
		return nil, errgo.Newf("entity missing charm metadata")
	}

	var resources []resource.Resource
	for _, meta := range entity.CharmMeta.Resources {
		// We use an origin of "upload" since resources cannot be uploaded yet.
		resOrigin := resource.OriginUpload
		res := resource.Resource{
			Meta:   meta,
			Origin: resOrigin,
			// Revision, Fingerprint, and Size are not set.
		}
		resources = append(resources, res)
	}
	resource.Sort(resources)
	return resources, nil
}

// POST id/resource/name
// https://github.com/juju/charmstore/blob/v5/docs/API.md#post-idresourcesname
//
// GET  id/resource/name[/revision]
// https://github.com/juju/charmstore/blob/v5/docs/API.md#get-idresourcesnamerevision
func (h *ReqHandler) serveResources(id *router.ResolvedURL, w http.ResponseWriter, req *http.Request) error {
	// Resources are "published" using "POST id/publish" so we don't
	// support PUT here.
	// TODO(ericsnow) Support DELETE to remove a resource (like serveArchive)?
	switch req.Method {
	case "GET":
		return h.serveDownloadResource(id, w, req)
	case "POST":
		return h.serveUploadResource(id, w, req)
	default:
		return errgo.WithCausef(nil, params.ErrMethodNotAllowed, "%s not allowed", req.Method)
	}
}

func (h *ReqHandler) serveDownloadResource(id *router.ResolvedURL, w http.ResponseWriter, req *http.Request) error {
	rid, err := parseResourceId(strings.TrimPrefix(req.URL.Path, "/"))
	if err != nil {
		return errgo.WithCausef(err, params.ErrNotFound, "")
	}
	ch, err := h.entityChannel(id)
	if err != nil {
		return errgo.Mask(err, errgo.Is(params.ErrNotFound))
	}
	r, err := h.Store.ResolveResource(id, rid.Name, rid.Revision, ch)
	if err != nil {
		return errgo.Mask(err, errgo.Is(params.ErrNotFound))
	}
	blob, err := h.Store.OpenResourceBlob(r)
	if err != nil {
		return errgo.Notef(err, "cannot open resource blob")
	}
	defer blob.Close()
	header := w.Header()
	setArchiveCacheControl(w.Header(), h.isPublic(id))
	header.Set(params.ContentHashHeader, blob.Hash)

	// TODO(rog) should we set connection=close here?
	// See https://codereview.appspot.com/5958045
	serveContent(w, req, blob.Size, blob)
	return nil
}

func (h *ReqHandler) serveUploadResource(id *router.ResolvedURL, w http.ResponseWriter, req *http.Request) error {
	if id.URL.Series == "bundle" {
		return errgo.WithCausef(nil, params.ErrForbidden, "cannot upload a resource to a bundle")
	}
	name := strings.TrimPrefix(req.URL.Path, "/")
	if !validResourceName(name) {
		return badRequestf(nil, "invalid resource name")
	}
	hash := req.Form.Get("hash")
	if hash == "" {
		return badRequestf(nil, "hash parameter not specified")
	}
	if req.ContentLength == -1 {
		return badRequestf(nil, "Content-Length not specified")
	}
	e, err := h.Cache.Entity(&id.URL, charmstore.FieldSelector("charmmeta"))
	if err != nil {
		// Should never happen, as the entity will have been cached
		// when the charm URL was resolved.
		return errgo.Mask(err, errgo.Is(params.ErrNotFound))
	}
	r, ok := e.CharmMeta.Resources[name]
	if !ok {
		return errgo.WithCausef(nil, params.ErrForbidden, "resource %q not found in charm metadata", name)
	}
	if r.Type != resource.TypeFile {
		return errgo.WithCausef(nil, params.ErrForbidden, "non-file resource types not supported")
	}
	if filename := req.Form.Get("filename"); filename != "" {
		if charmExt := path.Ext(r.Path); charmExt != "" {
			// The resource has a filename extension. Check that it matches.
			if charmExt != path.Ext(filename) {
				return errgo.WithCausef(nil, params.ErrForbidden, "filename extension mismatch (got %q want %q)", path.Ext(filename), charmExt)
			}
		}
	}
	rdoc, err := h.Store.UploadResource(e, name, req.Body, hash, req.ContentLength)
	if err != nil {
		return errgo.Mask(err)
	}
	return httprequest.WriteJSON(w, http.StatusOK, &params.ResourceUploadResponse{
		Revision: rdoc.Revision,
	})
}

func parseResourceId(path string) (mongodoc.ResourceRevision, error) {
	i := strings.Index(path, "/")
	if i == -1 {
		return mongodoc.ResourceRevision{
			Name:     path,
			Revision: -1,
		}, nil
	}
	revno, err := strconv.Atoi(path[i+1:])
	if err != nil {
		return mongodoc.ResourceRevision{}, errgo.Newf("malformed revision number")
	}
	return mongodoc.ResourceRevision{
		Name:     path[0:i],
		Revision: revno,
	}, nil
}

func validResourceName(name string) bool {
	// TODO we should probably be more restrictive than this.
	return !strings.Contains(name, "/")
}
