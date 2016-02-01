// Copyright 2015 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package v4 // import "gopkg.in/juju/charmstore.v5-unstable/internal/v4"

import (
	"net/http"
	"net/url"

	"github.com/juju/httprequest"
	"github.com/juju/loggo"
	"gopkg.in/errgo.v1"
	"gopkg.in/juju/charm.v6-unstable"
	"gopkg.in/juju/charmrepo.v2-unstable/csclient/params"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"gopkg.in/juju/charmstore.v5-unstable/internal/charmstore"
	"gopkg.in/juju/charmstore.v5-unstable/internal/entitycache"
	"gopkg.in/juju/charmstore.v5-unstable/internal/mempool"
	"gopkg.in/juju/charmstore.v5-unstable/internal/mongodoc"
	"gopkg.in/juju/charmstore.v5-unstable/internal/router"
	"gopkg.in/juju/charmstore.v5-unstable/internal/v5"
)

var logger = loggo.GetLogger("charmstore.internal.v4")

const (
	PromulgatorsGroup         = v5.PromulgatorsGroup
	UsernameAttr              = v5.UsernameAttr
	DelegatableMacaroonExpiry = v5.DelegatableMacaroonExpiry
	DefaultIcon               = v5.DefaultIcon
	ArchiveCachePublicMaxAge  = v5.ArchiveCachePublicMaxAge
)

// reqHandlerPool holds a cache of ReqHandlers to save
// on allocation time. When a handler is done with,
// it is put back into the pool.
var reqHandlerPool = mempool.Pool{
	New: func() interface{} {
		return newReqHandler()
	},
}

type Handler struct {
	*v5.Handler
}

type ReqHandler struct {
	*v5.ReqHandler
}

func New(pool *charmstore.Pool, config charmstore.ServerParams, rootPath string) Handler {
	return Handler{
		Handler: v5.New(pool, config, rootPath),
	}
}

func (h Handler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	rh, err := h.NewReqHandler()
	if err != nil {
		router.WriteError(w, err)
		return
	}
	defer rh.Close()
	rh.ServeHTTP(w, req)
}

func NewAPIHandler(pool *charmstore.Pool, config charmstore.ServerParams, rootPath string) charmstore.HTTPCloseHandler {
	return New(pool, config, rootPath)
}

// The v4 resolvedURL function also requires SupportedSeries.
var requiredEntityFields = func() map[string]int {
	fields := make(map[string]int)
	for f := range v5.RequiredEntityFields {
		fields[f] = 1
	}
	fields["supportedseries"] = 1
	return fields
}()

// NewReqHandler fetchs a new instance of ReqHandler
// from h.Pool and returns it. The ReqHandler must
// be closed when finished with.
func (h *Handler) NewReqHandler() (ReqHandler, error) {
	store, err := h.Pool.RequestStore()
	if err != nil {
		if errgo.Cause(err) == charmstore.ErrTooManySessions {
			return ReqHandler{}, errgo.WithCausef(err, params.ErrServiceUnavailable, "")
		}
		return ReqHandler{}, errgo.Mask(err)
	}
	rh := reqHandlerPool.Get().(ReqHandler)
	rh.Handler = h.Handler
	rh.Store = store
	rh.Cache = entitycache.New(store)
	rh.Cache.AddEntityFields(requiredEntityFields)
	rh.Cache.AddBaseEntityFields(v5.RequiredBaseEntityFields)
	return rh, nil
}

func newReqHandler() ReqHandler {
	h := ReqHandler{
		ReqHandler: new(v5.ReqHandler),
	}
	resolveId := h.ResolvedIdHandler
	authId := h.AuthIdHandler
	handlers := v5.RouterHandlers(h.ReqHandler)
	handlers.Global["search"] = router.HandleJSON(h.serveSearch)
	handlers.Meta["charm-related"] = h.EntityHandler(h.metaCharmRelated, "charmprovidedinterfaces", "charmrequiredinterfaces")
	handlers.Meta["revision-info"] = router.SingleIncludeHandler(h.metaRevisionInfo)
	handlers.Id["expand-id"] = resolveId(authId(h.serveExpandId))

	h.Router = router.New(handlers, h)
	return h
}

// ResolveURL implements router.Context.ResolveURL,
// ensuring that any resulting ResolvedURL always
// has a non-empty PreferredSeries field.
func (h ReqHandler) ResolveURL(url *charm.URL) (*router.ResolvedURL, error) {
	return resolveURL(h.Cache, url)
}

func (h ReqHandler) ResolveURLs(urls []*charm.URL) ([]*router.ResolvedURL, error) {
	h.Cache.StartFetch(urls)
	rurls := make([]*router.ResolvedURL, len(urls))
	for i, url := range urls {
		var err error
		rurls[i], err = resolveURL(h.Cache, url)
		if err != nil && errgo.Cause(err) != params.ErrNotFound {
			return nil, err
		}
	}
	return rurls, nil
}

// resolveURL implements URL resolving for the ReqHandler.
// It's defined as a separate function so it can be more
// easily unit-tested.
func resolveURL(cache *entitycache.Cache, url *charm.URL) (*router.ResolvedURL, error) {
	entity, err := cache.Entity(url, nil)
	if err != nil && errgo.Cause(err) != params.ErrNotFound {
		return nil, errgo.Mask(err)
	}
	if errgo.Cause(err) == params.ErrNotFound {
		return nil, noMatchingURLError(url)
	}
	rurl := &router.ResolvedURL{
		URL:                 *entity.URL,
		PromulgatedRevision: -1,
		Development:         (url.Channel == charm.DevelopmentChannel) || (entity.Development && !entity.Stable),
	}
	if url.User == "" {
		rurl.PromulgatedRevision = entity.PromulgatedRevision
	}
	if rurl.URL.Series != "" {
		return rurl, nil
	}
	if url.Series != "" {
		rurl.PreferredSeries = url.Series
		return rurl, nil
	}
	if len(entity.SupportedSeries) == 0 {
		return nil, errgo.Newf("entity %q has no supported series", &rurl.URL)
	}
	rurl.PreferredSeries = entity.SupportedSeries[0]
	return rurl, nil
}

// Close closes the ReqHandler. This should always be called when the
// ReqHandler is done with.
func (h ReqHandler) Close() {
	h.Store.Close()
	h.Cache.Close()
	h.Reset()
	reqHandlerPool.Put(h)
}

// StatsEnabled reports whether statistics should be gathered for
// the given HTTP request.
func StatsEnabled(req *http.Request) bool {
	return v5.StatsEnabled(req)
}

func noMatchingURLError(url *charm.URL) error {
	return errgo.WithCausef(nil, params.ErrNotFound, "no matching charm or bundle for %q", url)
}

// GET id/meta/revision-info
// https://github.com/juju/charmstore/blob/v4/docs/API.md#get-idmetarevision-info
func (h *ReqHandler) metaRevisionInfo(id *router.ResolvedURL, path string, flags url.Values, req *http.Request) (interface{}, error) {
	searchURL := id.PreferredURL()
	searchURL.Revision = -1

	q := h.Store.EntitiesQuery(searchURL)
	if id.PromulgatedRevision != -1 {
		q = q.Sort("-promulgated-revision")
	} else {
		q = q.Sort("-revision")
	}
	var docs []*mongodoc.Entity
	if err := q.Select(bson.D{{"_id", 1}, {"promulgated-url", 1}, {"supportedseries", 1}, {"development", 1}}).All(&docs); err != nil {
		return "", errgo.Notef(err, "cannot get ids")
	}

	if len(docs) == 0 {
		return "", errgo.WithCausef(nil, params.ErrNotFound, "no matching charm or bundle for %s", id)
	}
	specifiedSeries := id.URL.Series
	if specifiedSeries == "" {
		specifiedSeries = id.PreferredSeries
	}
	var response params.RevisionInfoResponse
	expandMultiSeries(docs, func(series string, doc *mongodoc.Entity) error {
		if specifiedSeries != series {
			return nil
		}
		url := doc.PreferredURL(id.PromulgatedRevision != -1)
		url.Series = series
		response.Revisions = append(response.Revisions, url)
		return nil
	})
	return &response, nil
}

// GET id/expand-id
// https://docs.google.com/a/canonical.com/document/d/1TgRA7jW_mmXoKH3JiwBbtPvQu7WiM6XMrz1wSrhTMXw/edit#bookmark=id.4xdnvxphb2si
func (h *ReqHandler) serveExpandId(id *router.ResolvedURL, w http.ResponseWriter, req *http.Request) error {
	baseURL := id.PreferredURL()
	baseURL.Revision = -1
	baseURL.Series = ""

	// baseURL now represents the base URL of the given id;
	// it will be a promulgated URL iff the original URL was
	// specified without a user, which will cause EntitiesQuery
	// to return entities that match appropriately.

	// Retrieve all the entities with the same base URL.
	// Note that we don't do any permission checking of the returned URLs.
	// This is because we know that the user is allowed to read at
	// least the resolved URL passed into serveExpandId.
	// If this does not specify "development", then no development
	// revisions will be chosen, so the single ACL already checked
	// is sufficient. If it *does* specify "development", then we assume
	// that the development ACLs are more restrictive than the
	// non-development ACLs, and given that, we can allow all
	// the URLs.
	q := h.Store.EntitiesQuery(baseURL).Select(bson.D{{"_id", 1}, {"promulgated-url", 1}, {"development", 1}, {"supportedseries", 1}})
	if id.PromulgatedRevision != -1 {
		q = q.Sort("-series", "-promulgated-revision")
	} else {
		q = q.Sort("-series", "-revision")
	}
	var docs []*mongodoc.Entity
	err := q.All(&docs)
	if err != nil && errgo.Cause(err) != mgo.ErrNotFound {
		return errgo.Mask(err)
	}

	// Collect all the expanded identifiers for each entity.
	response := make([]params.ExpandedId, 0, len(docs))
	expandMultiSeries(docs, func(series string, doc *mongodoc.Entity) error {
		url := doc.PreferredURL(id.PromulgatedRevision != -1)
		url.Series = series
		response = append(response, params.ExpandedId{Id: url.String()})
		return nil
	})

	// Write the response in JSON format.
	return httprequest.WriteJSON(w, http.StatusOK, response)
}

// expandMultiSeries calls the provided append function once for every
// supported series of each entry in the given entities slice. The series
// argument will be passed as that series and the doc argument will point
// to the entity. This function will only return an error if the append
// function returns an error; such an error will be returned without
// masking the cause.
//
// Note that the SupportedSeries field of the entities must have
// been populated for this to work.
func expandMultiSeries(entities []*mongodoc.Entity, append func(series string, doc *mongodoc.Entity) error) error {
	// TODO(rog) make this concurrent.
	for _, entity := range entities {
		if entity.URL.Series != "" {
			append(entity.URL.Series, entity)
			continue
		}
		for _, series := range entity.SupportedSeries {
			if err := append(series, entity); err != nil {
				return errgo.Mask(err, errgo.Any)
			}
		}
	}
	return nil
}
