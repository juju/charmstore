// Copyright 2015 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package charmstore // import "gopkg.in/juju/charmstore.v5-unstable/internal/charmstore"

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"time"

	"github.com/juju/httprequest"
	"github.com/juju/utils"
	"github.com/juju/utils/debugstatus"
	"github.com/julienschmidt/httprouter"
	"gopkg.in/errgo.v1"
	"gopkg.in/juju/charmrepo.v2-unstable/csclient/params"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"gopkg.in/juju/charmstore.v5-unstable/internal/mongodoc"
	"gopkg.in/juju/charmstore.v5-unstable/internal/router"
	appver "gopkg.in/juju/charmstore.v5-unstable/version"
)

type debugParams struct {
	ServerParams
	pool            *Pool

	// loopbackHandler contains the handler for / it is used by
	// /debug/fullcheck when performing tests.
	loopbackHandler http.Handler
}

type debugHandler struct {
	// router contains an httprouter.Router that is used to process
	// calls to the debug endpoints.
	router    *httprouter.Router

	// params containst the parameters that the debugHandler was
	// created with.
	params    debugParams

	// checks contains the list of checks used in /debug/check.
	checks    map[string]func() error

	// openPaths contains the /debug endpoints that are not protected
	// by authorization.
	openPaths map[string]bool
}

// newDebugHandler creates a new handler for serving the /debug tree.
func newDebugHandler(p debugParams) *debugHandler {
	hnd := debugHandler{
		router: httprouter.New(),
		params: p,
		checks: map[string]func() error{
			"mongodb":       checkDB(p.pool.db.Database),
			"elasticsearch": checkES(p.pool.es),
		},
		openPaths: map[string]bool{
			"/debug/check":  true,
			"/debug/info":   true,
			"/debug/status": true,
		},
	}
	for _, h := range router.ErrorToResp.Handlers(hnd.handle) {
		hnd.router.Handle(h.Method, h.Path, h.Handle)
	}
	return &hnd
}

func (h *debugHandler) handle(p httprequest.Params) (*debugReqHandler, error) {
	if err := h.authorized(p.Request); err != nil {
		return nil, err
	}
	rh := debugReqHandler{
		Handler: debugstatus.Handler{
			Version: debugstatus.Version(appver.VersionInfo),
			// Authorization has alread been checked so there is no need to do it again.
			CheckPprofAllowed: func(*http.Request) error { return nil },
		},
		h: h,
	}
	rh.Handler.Check = rh.check
	return &rh, nil
}

// authorized checks the debug requests for the required authorization.
// If openPaths is true for the requested path, then no authorization
// will occur.
func (h *debugHandler) authorized(r *http.Request) error {
	if h.openPaths[r.URL.Path] {
		return nil
	}
	// TODO support other login methods
	u, p, err := utils.ParseBasicAuthHeader(r.Header)
	if err != nil {
		return errgo.WithCausef(err, params.ErrUnauthorized, "")
	}
	if u != h.params.AuthUsername || p != h.params.AuthPassword {
		return errgo.WithCausef(nil, params.ErrUnauthorized, "username or password mismatch")
	}
	return nil
}

func (h *debugHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// The charmstore router will have stripped "/debug" off the
	// front of the path; put it back so that the router can route
	// the paths from debugstatus.Handler correctly.
	r.URL.Path = "/debug" + r.URL.Path
	h.router.ServeHTTP(w, r)
}

type debugReqHandler struct {
	debugstatus.Handler
	h *debugHandler

	// store is initialized lazily.
	store_     *Store
	storeError error
}

func (h *debugReqHandler) Close() error {
	if h.store_ != nil {
		h.store_.Close()
	}
	return nil
}

// store lazily retrieves a store from the pool.
func (h *debugReqHandler) store() (*Store, error) {
	if h.store_ == nil && h.storeError == nil {
		h.store_, h.storeError = h.h.params.pool.RequestStore()
	}
	return h.store_, h.storeError
}

// check implements the Check function that is used with
// debugstatus.Handler.Check.
func (h *debugReqHandler) check() map[string]debugstatus.CheckResult {
	checkers := []debugstatus.CheckerFunc{
		debugstatus.ServerStartTime,
		h.checkElasticSearch,
	}
	store, err := h.store()
	if err != nil {
		checkers = append(checkers, func() (string, debugstatus.CheckResult) {
			return "store available", debugstatus.CheckResult{
				Name:   "Get Store From Pool",
				Value:  err.Error(),
				Passed: false,
			}
		})
	} else {
		checkers = append(checkers,
			debugstatus.Connection(store.DB.Session),
			debugstatus.MongoCollections(store.DB),
			h.checkEntities,
			h.checkBaseEntities,
			h.checkLogs(
				"ingestion", "Ingestion",
				mongodoc.IngestionType,
				params.IngestionStart, params.IngestionComplete,
			),
			h.checkLogs(
				"legacy_statistics", "Legacy Statistics Load",
				mongodoc.LegacyStatisticsType,
				params.LegacyStatisticsImportStart, params.LegacyStatisticsImportComplete,
			),
		)
	}
	return debugstatus.Check(checkers...)
}

// checkElasticSearch checks the elasticsearch connection for /debug/status.
func (h *debugReqHandler) checkElasticSearch() (key string, result debugstatus.CheckResult) {
	key = "elasticsearch"
	result.Name = "Elastic search is running"
	si := h.h.params.pool.es
	if si == nil || si.Database == nil {
		result.Value = "Elastic search is not configured"
		result.Passed = true
		return key, result
	}
	health, err := si.Health()
	if err != nil {
		result.Value = "Connection issues to Elastic Search: " + err.Error()
		return key, result
	}
	result.Value = health.String()
	result.Passed = health.Status == "green"
	return key, result
}

// checkEntities checks the entities database for consistency.
func (h *debugReqHandler) checkEntities() (key string, result debugstatus.CheckResult) {
	result.Name = "Entities in charm store"
	charms, err := h.store_.DB.Entities().Find(bson.D{{"series", bson.D{{"$ne", "bundle"}}}}).Count()
	if err != nil {
		result.Value = "Cannot count charms for consistency check: " + err.Error()
		return "entities", result
	}
	bundles, err := h.store_.DB.Entities().Find(bson.D{{"series", "bundle"}}).Count()
	if err != nil {
		result.Value = "Cannot count bundles for consistency check: " + err.Error()
		return "entities", result
	}
	promulgated, err := h.store_.DB.Entities().Find(bson.D{{"promulgated-url", bson.D{{"$exists", true}}}}).Count()
	if err != nil {
		result.Value = "Cannot count promulgated for consistency check: " + err.Error()
		return "entities", result
	}
	result.Value = fmt.Sprintf("%d charms; %d bundles; %d promulgated", charms, bundles, promulgated)
	result.Passed = true
	return "entities", result
}

// checkBaseEntities checks the base entities database for consistency.
func (h *debugReqHandler) checkBaseEntities() (key string, result debugstatus.CheckResult) {
	resultKey := "base_entities"
	result.Name = "Base entities in charm store"

	// Retrieve the number of base entities.
	baseNum, err := h.store_.DB.BaseEntities().Count()
	if err != nil {
		result.Value = "Cannot count base entities: " + err.Error()
		return resultKey, result
	}

	// Retrieve the number of entities.
	num, err := h.store_.DB.Entities().Count()
	if err != nil {
		result.Value = "Cannot count entities for consistency check: " + err.Error()
		return resultKey, result
	}

	result.Value = fmt.Sprintf("count: %d", baseNum)
	result.Passed = num >= baseNum
	return resultKey, result
}

// checkLogs creates a function that will check for the last time the
// logs contain a start and end for an operation.
func (h *debugReqHandler) checkLogs(
	resultKey, resultName string,
	logType mongodoc.LogType,
	startPrefix, endPrefix string,
) debugstatus.CheckerFunc {
	return func() (key string, result debugstatus.CheckResult) {
		result.Name = resultName
		start, end, err := h.findTimesInLogs(logType, startPrefix, endPrefix)
		if err != nil {
			result.Value = err.Error()
			return resultKey, result
		}
		result.Value = fmt.Sprintf("started: %s, completed: %s", start.Format(time.RFC3339), end.Format(time.RFC3339))
		result.Passed = !(start.IsZero() || end.IsZero())
		return resultKey, result
	}
}

// findTimesInLogs goes through logs in reverse order finding when the start and
// end messages were last added.
func (h *debugReqHandler) findTimesInLogs(logType mongodoc.LogType, startPrefix, endPrefix string) (start, end time.Time, err error) {
	var log mongodoc.Log
	iter := h.store_.DB.Logs().
		Find(bson.D{
		{"level", mongodoc.InfoLevel},
		{"type", logType},
	}).Sort("-time", "-id").Iter()
	for iter.Next(&log) {
		var msg string
		if err := json.Unmarshal(log.Data, &msg); err != nil {
			// an error here probably means the log isn't in the form we are looking for.
			continue
		}
		if start.IsZero() && strings.HasPrefix(msg, startPrefix) {
			start = log.Time
		}
		if end.IsZero() && strings.HasPrefix(msg, endPrefix) {
			end = log.Time
		}
		if !start.IsZero() && !end.IsZero() {
			break
		}
	}
	if err = iter.Close(); err != nil {
		return time.Time{}, time.Time{}, errgo.Notef(err, "Cannot query logs")
	}
	return
}

// debugCheckRequest represents a request to the /debug/check endpoint. 
type debugCheckRequest struct {
	httprequest.Route `httprequest:"GET /debug/check"`
}

// Check handles a request to the /debug/check endpoint. These are very
// simple checks that check the connection to external services are up.
func (h *debugReqHandler) Check(*debugCheckRequest) (map[string]string, error) {
	n := len(h.h.checks)
	type result struct {
		name string
		err  error
	}
	c := make(chan result)
	for name, check := range h.h.checks {
		name, check := name, check
		go func() {
			c <- result{name: name, err: check()}
		}()
	}
	results := make(map[string]string, n)
	var failed bool
	for ; n > 0; n-- {
		res := <-c
		if res.err == nil {
			results[res.name] = "OK"
		} else {
			failed = true
			results[res.name] = res.err.Error()
		}
	}
	if failed {
		keys := make([]string, 0, len(results))
		for k := range results {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		msgs := make([]string, len(results))
		for i, k := range keys {
			msgs[i] = fmt.Sprintf("[%s: %s]", k, results[k])
		}
		return nil, errgo.Newf("check failure: %s", strings.Join(msgs, " "))
	}
	return results, nil
}

// checkDB checks the connection to a mongodb database.
func checkDB(db *mgo.Database) func() error {
	return func() error {
		s := db.Session.Copy()
		s.SetSyncTimeout(500 * time.Millisecond)
		defer s.Close()
		return s.Ping()
	}
}

// checkES checks the connection to an elasticsearch database.
func checkES(si *SearchIndex) func() error {
	if si == nil || si.Database == nil {
		return func() error {
			return nil
		}
	}
	return func() error {
		_, err := si.Health()
		return err
	}
}

// debugFullCheckRequest represents a request to the /debug/fullcheck endpoint.
type debugFullCheckRequest struct {
	httprequest.Route `httprequest:"GET /debug/fullcheck"`
}

// FullCheck handles a request to the /debug/fullcheck endpoint. This
// does a comprehensive check of a number of operations on the charm
// store.
func (h *debugReqHandler) FullCheck(p httprequest.Params, _ *debugFullCheckRequest) {
	w := p.Response
	hnd := h.h.params.loopbackHandler
	code := http.StatusInternalServerError
	resp := new(bytes.Buffer)
	defer func() {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(code)
		resp.WriteTo(w)
	}()

	fmt.Fprintln(resp, "Testing v4...")

	// test search
	fmt.Fprintln(resp, "performing search...")
	var sr params.SearchResponse
	if err := get(hnd, "/v4/search?limit=2000", &sr); err != nil {
		fmt.Fprintf(resp, "ERROR: search failed %s.\n", err)
		return
	}
	if len(sr.Results) < 1 {
		fmt.Fprintln(resp, "ERROR: no search results found.")
		return
	}
	fmt.Fprintf(resp, "%d results found.\n", len(sr.Results))

	// pick random charm
	id := sr.Results[rand.Intn(len(sr.Results))].Id
	fmt.Fprintf(resp, "using %s.\n", id)

	// test content
	fmt.Fprintln(resp, "reading manifest...")
	url := "/v4/" + id.Path() + "/meta/manifest"
	fmt.Fprintln(resp, url)
	var files []params.ManifestFile
	if err := get(hnd, url, &files); err != nil {
		fmt.Fprintf(resp, "ERROR: cannot retrieve manifest: %s.\n", err)
		return
	}
	if len(files) == 0 {
		fmt.Fprintln(resp, "ERROR: manifest empty.")
		return
	}
	fmt.Fprintf(resp, "%d files found.\n", len(files))

	// Choose a file to access
	expectFile := "metadata.yaml"
	if id.Series == "bundle" {
		expectFile = "bundle.yaml"
	}
	var file params.ManifestFile
	// default to metadata.yaml
	for _, f := range files {
		if f.Name == expectFile {
			file = f
			break
		}
	}
	// find a random file
	for i := 0; i < 5; i++ {
		f := files[rand.Intn(len(files))]
		if f.Size <= 16*1024 {
			file = f
			break
		}
	}
	fmt.Fprintf(resp, "using %s.\n", file.Name)

	// read the file
	fmt.Fprintln(resp, "reading file...")
	url = "/v4/" + id.Path() + "/archive/" + file.Name
	fmt.Fprintln(resp, url)
	var buf []byte
	if err := get(hnd, url, &buf); err != nil {
		fmt.Fprintf(resp, "ERROR: cannot retrieve file: %s.\n", err)
		return
	}
	if int64(len(buf)) != file.Size {
		fmt.Fprintf(resp, "ERROR: incorrect file size, expected: %d, received %d.\n", file.Size, len(buf))
		return
	}
	fmt.Fprintf(resp, "%d bytes received.\n", len(buf))

	// check if the charm is promulgated
	fmt.Fprintln(resp, "checking promulgated...")
	url = "/v4/" + id.Path() + "/meta/promulgated"
	fmt.Fprintln(resp, url)
	var promulgated params.PromulgatedResponse
	if err := get(hnd, url, &promulgated); err != nil {
		fmt.Fprintf(resp, "ERROR: cannot retrieve promulgated: %s.\n", err)
		return
	}
	if promulgated.Promulgated != (id.User == "") {
		fmt.Fprintf(resp, "ERROR: incorrect promulgated response, expected: %v, received %v.\n", (id.User == ""), promulgated.Promulgated)
		return
	}
	fmt.Fprintf(resp, "promulgated: %v.\n", promulgated.Promulgated)

	// check expand-id
	fmt.Fprintln(resp, "checking expand-id...")
	url = "/v4/" + id.Path() + "/expand-id"
	fmt.Fprintln(resp, url)
	var expanded []params.ExpandedId
	if err := get(hnd, url, &expanded); err != nil {
		fmt.Fprintf(resp, "ERROR: cannot expand-id: %s.\n", err)
		return
	}
	if len(expanded) == 0 {
		fmt.Fprintln(resp, "ERROR: expand-id returned 0 results")
		return
	}
	fmt.Fprintf(resp, "%d ids found.\n", len(expanded))

	code = http.StatusOK
}

// get is a helper function for FullCheck that performs get requests on a handler.
func get(h http.Handler, url string, body interface{}) error {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return errgo.Notef(err, "cannot create request")
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		if w.HeaderMap.Get("Content-Type") != "application/json" {
			return errgo.Newf("bad status %d", w.Code)
		}
		var e params.Error
		if err := json.Unmarshal(w.Body.Bytes(), &e); err != nil {
			return errgo.Notef(err, "cannot decode error")
		}
		return &e
	}
	if body == nil {
		return nil
	}
	if bytes, ok := body.(*[]byte); ok {
		*bytes = w.Body.Bytes()
		return nil
	}
	if w.HeaderMap.Get("Content-Type") == "application/json" {
		if err := json.Unmarshal(w.Body.Bytes(), body); err != nil {
			return errgo.Notef(err, "cannot decode body")
		}
		return nil
	}
	return errgo.Newf("cannot decode body")
}
