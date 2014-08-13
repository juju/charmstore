// Copyright 2014 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package v4

import (
	"archive/zip"
	"net/http"
	"net/url"

	"github.com/juju/errgo"
	"gopkg.in/juju/charm.v3"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/juju/charmstore/internal/charmstore"
	"github.com/juju/charmstore/internal/mongodoc"
	"github.com/juju/charmstore/internal/router"
	"github.com/juju/charmstore/params"
)

type handler struct {
	*router.Router
	store *charmstore.Store
}

// New returns a new instance of the v4 API handler.
func New(store *charmstore.Store) http.Handler {
	h := &handler{
		store: store,
	}
	h.Router = router.New(&router.Handlers{
		Global: map[string]http.Handler{
			"stats/counter/":     router.HandleJSON(h.serveStatsCounter),
			"stats/":             router.NotFoundHandler(),
			"search":             http.HandlerFunc(h.serveSearch),
			"search/interesting": http.HandlerFunc(h.serveSearchInteresting),
			"debug":              http.HandlerFunc(h.serveDebug),
		},
		Id: map[string]router.IdHandler{
			"resources": h.serveResources,
			"archive":   h.serveArchive,
			"archive/":  h.serveArchiveFile,
			"expand-id": h.serveExpandId,
		},
		Meta: map[string]router.BulkIncludeHandler{
			"charm-metadata":      h.entityHandler(h.metaCharmMetadata, "charmmeta"),
			"bundle-metadata":     h.entityHandler(h.metaBundleMetadata, "bundledata"),
			"charm-config":        h.entityHandler(h.metaCharmConfig, "charmconfig"),
			"charm-actions":       h.entityHandler(h.metaCharmActions, "charmactions"),
			"archive-size":        h.entityHandler(h.metaArchiveSize, "size"),
			"manifest":            h.entityHandler(h.metaManifest, "blobname"),
			"archive-upload-time": h.entityHandler(h.metaArchiveUploadTime, "uploadtime"),

			// endpoints not yet implemented - use SingleIncludeHandler for the time being.
			"color":              router.SingleIncludeHandler(h.metaColor),
			"bundles-containing": router.SingleIncludeHandler(h.metaBundlesContaining),
			"extra-info":         router.SingleIncludeHandler(h.metaExtraInfo),
			"extra-info/":        router.SingleIncludeHandler(h.metaExtraInfoWithKey),
			"charm-related":      router.SingleIncludeHandler(h.metaCharmRelated),
		},
	}, h.resolveURL)
	return h
}

func (h *handler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	h.Router.ServeHTTP(w, req)
}

// ResolveURL resolves the series and revision of the given URL
// if either is unspecified by filling them out with information retrieved
// from the store.
func ResolveURL(store *charmstore.Store, url *charm.Reference) error {
	if url.Series != "" && url.Revision != -1 {
		return nil
	}
	urls, err := store.ExpandURL(url)
	if err != nil {
		return errgo.Notef(err, "cannot expand URL")
	}
	if len(urls) == 0 {
		return errgo.WithCausef(nil, params.ErrNotFound, "no matching charm or bundle for %q", url)
	}
	*url = *selectPreferredURL(urls)
	return nil
}

func (h *handler) resolveURL(url *charm.Reference) error {
	return ResolveURL(h.store, url)
}

type entityHandlerFunc func(entity *mongodoc.Entity, id *charm.Reference, path string, method string, flags url.Values) (interface{}, error)

// entityHandler returns a handler that calls f with a *mongodoc.Entity that
// contains at least the given fields. It allows only GET requests.
func (h *handler) entityHandler(f entityHandlerFunc, fields ...string) router.BulkIncludeHandler {
	return h.entityHandler0(f, false, fields...)
}

// entityHandler returns a handler that calls f with a *mongodoc.Entity that
// contains at least the given fields. It allows GET and PUT requests.
func (h *handler) entityHandlerWithPut(f entityHandlerFunc, fields ...string) router.BulkIncludeHandler {
	return h.entityHandler0(f, true, fields...)
}

// entityHandler0 holds the implementation for both entityHandler
// and entityHandlerWithPut.
func (h *handler) entityHandler0(f entityHandlerFunc, allowPut bool, fields ...string) router.BulkIncludeHandler {
	handle := func(doc interface{}, id *charm.Reference, path string, method string, flags url.Values) (interface{}, error) {
		edoc := doc.(*mongodoc.Entity)
		val, err := f(edoc, id, path, method, flags)
		return val, errgo.Mask(err, errgo.Any)
	}
	type entityHandlerKey struct{}
	return &entityHandler{
		allowPut: allowPut,
		BulkIncludeHandler: router.FieldIncludeHandler(
			entityHandlerKey{},
			h.entityQuery,
			fields,
			handle,
		),
	}
}

type entityHandler struct {
	allowPut bool
	router.BulkIncludeHandler
}

func (h *entityHandler) Handle(hs []router.BulkIncludeHandler, id *charm.Reference, paths []string, method string, flags url.Values) ([]interface{}, error) {
	methodOk := method == "GET" || (h.allowPut && method == "PUT")
	if !methodOk {
		return nil, errgo.WithCausef(nil, params.ErrBadRequest, "method %q not allowed", method)
	}
	// Transform the values in the slice so that the slice
	// is suitable for passing to the FieldIncludeHandler.
	hs1 := make([]router.BulkIncludeHandler, len(hs))
	for i, h := range hs {
		hs1[i] = h.(*entityHandler).BulkIncludeHandler
	}
	return h.BulkIncludeHandler.Handle(hs1, id, paths, method, flags)
}

func (h *handler) entityQuery(id *charm.Reference, selector map[string]int) (interface{}, error) {
	var val mongodoc.Entity
	err := h.store.DB.Entities().
		Find(bson.D{{"_id", id}}).
		Select(selector).
		One(&val)
	if err == mgo.ErrNotFound {
		return nil, params.ErrNotFound
	}
	if err != nil {
		return nil, errgo.Mask(err)
	}
	return &val, nil
}

var ltsReleases = map[string]bool{
	"lucid":   true,
	"precise": true,
	"trusty":  true,
}

func selectPreferredURL(urls []*charm.Reference) *charm.Reference {
	best := urls[0]
	for _, url := range urls {
		if preferredURL(url, best) {
			best = url
		}
	}
	return best
}

// preferredURL reports whether url0 is preferred over url1.
func preferredURL(url0, url1 *charm.Reference) bool {
	if url0.Series == url1.Series {
		return url0.Revision > url1.Revision
	}
	if url0.Series == "bundle" || url1.Series == "bundle" {
		// One of the URLs refers to a bundle. Choose
		// a charm by preference.
		return url0.Series != "bundle"
	}
	if ltsReleases[url0.Series] == ltsReleases[url1.Series] {
		return url0.Series > url1.Series
	}
	return ltsReleases[url0.Series]
}

var errNotImplemented = errgo.Newf("method not implemented")

// GET search[?text=text][&autocomplete=1][&filter=value…][&limit=limit][&include=meta]
// http://tinyurl.com/qzobc69
func (h *handler) serveSearch(w http.ResponseWriter, req *http.Request) {
	router.WriteError(w, errNotImplemented)
}

// GET search/interesting[?limit=limit][&include=meta]
// http://tinyurl.com/ntmdrg8
func (h *handler) serveSearchInteresting(w http.ResponseWriter, req *http.Request) {
	router.WriteError(w, errNotImplemented)
}

// GET /debug
// http://tinyurl.com/m63xhz8
func (h *handler) serveDebug(w http.ResponseWriter, req *http.Request) {
	router.WriteError(w, errNotImplemented)
}

// POST id/resources/name.stream
// http://tinyurl.com/pnmwvy4
//
// GET  id/resources/name.stream[-revision]/arch/filename
// http://tinyurl.com/pydbn3u
//
// PUT id/resources/[~user/]series/name.stream-revision/arch?sha256=hash
// http://tinyurl.com/k8l8kdg
func (h *handler) serveResources(charmId *charm.Reference, w http.ResponseWriter, req *http.Request) error {
	return errNotImplemented
}

// GET id/expand-id
// https://docs.google.com/a/canonical.com/document/d/1TgRA7jW_mmXoKH3JiwBbtPvQu7WiM6XMrz1wSrhTMXw/edit#bookmark=id.4xdnvxphb2si
func (h *handler) serveExpandId(charmId *charm.Reference, w http.ResponseWriter, req *http.Request) error {
	return errNotImplemented
}

func badRequestf(underlying error, f string, a ...interface{}) error {
	err := errgo.WithCausef(underlying, params.ErrBadRequest, f, a...)
	err.(*errgo.Err).SetLocation(1)
	return err
}

// GET id/meta/charm-metadata
// http://tinyurl.com/poeoulw
func (h *handler) metaCharmMetadata(entity *mongodoc.Entity, id *charm.Reference, path string, method string, flags url.Values) (interface{}, error) {
	return entity.CharmMeta, nil
}

// GET id/meta/bundle-metadata
// http://tinyurl.com/ozshbtb
func (h *handler) metaBundleMetadata(entity *mongodoc.Entity, id *charm.Reference, path string, method string, flags url.Values) (interface{}, error) {
	return entity.BundleData, nil
}

// GET id/meta/manifest
// http://tinyurl.com/p3xdcto
func (h *handler) metaManifest(entity *mongodoc.Entity, id *charm.Reference, path, method string, flags url.Values) (interface{}, error) {
	r, size, err := h.store.BlobStore.Open(entity.BlobName)
	if err != nil {
		return nil, errgo.Notef(err, "cannot open archive data for %s", id)
	}
	defer r.Close()
	zipReader, err := zip.NewReader(&readerAtSeeker{r}, size)
	if err != nil {
		return nil, errgo.Notef(err, "cannot read archive data for %s", id)
	}
	// Collect the files.
	manifest := make([]params.ManifestFile, 0, len(zipReader.File))
	for _, file := range zipReader.File {
		fileInfo := file.FileInfo()
		if fileInfo.IsDir() {
			continue
		}
		manifest = append(manifest, params.ManifestFile{
			Name: file.Name,
			Size: fileInfo.Size(),
		})
	}
	return manifest, nil
}

// GET id/meta/charm-actions
// http://tinyurl.com/kfd2h34
func (h *handler) metaCharmActions(entity *mongodoc.Entity, id *charm.Reference, path string, method string, flags url.Values) (interface{}, error) {
	return entity.CharmActions, nil
}

// GET id/meta/charm-config
// http://tinyurl.com/oxxyujx
func (h *handler) metaCharmConfig(entity *mongodoc.Entity, id *charm.Reference, path string, method string, flags url.Values) (interface{}, error) {
	return entity.CharmConfig, nil
}

// GET id/meta/color
// http://tinyurl.com/o2t3j4p
func (h *handler) metaColor(id *charm.Reference, path string, method string, flags url.Values) (interface{}, error) {
	return nil, errNotImplemented
}

// GET id/meta/archive-size
// http://tinyurl.com/m8b9geq
func (h *handler) metaArchiveSize(entity *mongodoc.Entity, id *charm.Reference, path string, method string, flags url.Values) (interface{}, error) {
	return &params.ArchiveSizeResponse{
		Size: entity.Size,
	}, nil
}

// GET id/meta/stats/
// http://tinyurl.com/lvyp2l5
func (h *handler) metaStats(id *charm.Reference, path string, method string, flags url.Values) (interface{}, error) {
	return nil, errNotImplemented
}

// GET id/meta/bundles-containing[?include=meta[&include=meta…]]
// http://tinyurl.com/oqc386r
func (h *handler) metaBundlesContaining(id *charm.Reference, path string, method string, flags url.Values) (interface{}, error) {
	return nil, errNotImplemented
}

// GET id/meta/extra-info
// http://tinyurl.com/keos7wd
func (h *handler) metaExtraInfo(id *charm.Reference, path string, method string, flags url.Values) (interface{}, error) {
	return nil, errNotImplemented
}

// GET id/meta/extra-info/key
// http://tinyurl.com/polrbn7
func (h *handler) metaExtraInfoWithKey(id *charm.Reference, path string, method string, flags url.Values) (interface{}, error) {
	return nil, errNotImplemented
}

// GET id/meta/charm-related[?include=meta[&include=meta…]]
// http://tinyurl.com/q7vdmzl
func (h *handler) metaCharmRelated(id *charm.Reference, path string, method string, flags url.Values) (interface{}, error) {
	return nil, errNotImplemented
}

// GET id/meta/archive-upload-time
// http://tinyurl.com/nmujuqk
func (h *handler) metaArchiveUploadTime(entity *mongodoc.Entity, id *charm.Reference, path, method string, flags url.Values) (interface{}, error) {
	return &params.ArchiveUploadTimeResponse{
		UploadTime: entity.UploadTime.UTC(),
	}, nil
}
