// Copyright 2015 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package charmstore_test

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/juju/testing"
	jc "github.com/juju/testing/checkers"
	"github.com/juju/testing/httptesting"
	"github.com/juju/utils/debugstatus"
	gc "gopkg.in/check.v1"
	"gopkg.in/juju/charm.v6-unstable"
	"gopkg.in/juju/charmrepo.v2-unstable/csclient/params"

	"gopkg.in/juju/charmstore.v5-unstable/internal/charmstore"
	"gopkg.in/juju/charmstore.v5-unstable/internal/mongodoc"
	"gopkg.in/juju/charmstore.v5-unstable/internal/router"
	"gopkg.in/juju/charmstore.v5-unstable/internal/storetesting"
	"gopkg.in/juju/charmstore.v5-unstable/internal/v4"
	"gopkg.in/juju/charmstore.v5-unstable/internal/v5"
	appver "gopkg.in/juju/charmstore.v5-unstable/version"
)

var serverParams = charmstore.ServerParams{
	AuthUsername: "test-admin",
	AuthPassword: "test-pass",
}

type debugSuite struct {
	testing.IsolatedMgoSuite
	srv *charmstore.Server
}

var _ = gc.Suite(&debugSuite{})

func (s *debugSuite) SetUpTest(c *gc.C) {
	s.IsolatedMgoSuite.SetUpTest(c)
	var err error
	s.srv, err = charmstore.NewServer(
		s.Session.DB("debug-testing"),
		nil,
		serverParams,
		map[string]charmstore.NewAPIHandlerFunc{
			"dummy": func(pool *charmstore.Pool, p charmstore.ServerParams, absPath string) charmstore.HTTPCloseHandler {
				return dummyHandler{}
			},
		},
	)
	c.Assert(err, gc.IsNil)
}

type dummyHandler struct{}

func (dummyHandler) ServeHTTP(http.ResponseWriter, *http.Request) {
	panic("request to dummy handler")
}

func (dummyHandler) Close() {
}

func (s *debugSuite) TearDownTest(c *gc.C) {
	s.srv.Close()
	s.IsolatedMgoSuite.TearDownTest(c)
}

func (s *debugSuite) TestDebugInfo(c *gc.C) {
	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler:      s.srv,
		URL:          "/debug/info",
		ExpectStatus: http.StatusOK,
		ExpectBody:   appver.VersionInfo,
	})
}

var zeroTimeStr = time.Time{}.Format(time.RFC3339)

var newResolvedURL = router.MustNewResolvedURL

func (s *debugSuite) TestStatus(c *gc.C) {
	store := s.srv.Pool().Store()
	defer store.Close()
	addEntitiesToStore(c, store)
	now := time.Now()
	s.PatchValue(&debugstatus.StartTime, now)
	start := now.Add(-2 * time.Hour)
	addLog(c, store, &mongodoc.Log{
		Data:  []byte(`"ingestion started"`),
		Level: mongodoc.InfoLevel,
		Type:  mongodoc.IngestionType,
		Time:  start,
	})
	end := now.Add(-1 * time.Hour)
	addLog(c, store, &mongodoc.Log{
		Data:  []byte(`"ingestion completed"`),
		Level: mongodoc.InfoLevel,
		Type:  mongodoc.IngestionType,
		Time:  end,
	})
	statisticsStart := now.Add(-1*time.Hour - 30*time.Minute)
	addLog(c, store, &mongodoc.Log{
		Data:  []byte(`"legacy statistics import started"`),
		Level: mongodoc.InfoLevel,
		Type:  mongodoc.LegacyStatisticsType,
		Time:  statisticsStart,
	})
	statisticsEnd := now.Add(-30 * time.Minute)
	addLog(c, store, &mongodoc.Log{
		Data:  []byte(`"legacy statistics import completed"`),
		Level: mongodoc.InfoLevel,
		Type:  mongodoc.LegacyStatisticsType,
		Time:  statisticsEnd,
	})
	s.AssertDebugStatus(c, true, map[string]params.DebugStatus{
		"mongo_connected": {
			Name:   "MongoDB is connected",
			Value:  "Connected",
			Passed: true,
		},
		"mongo_collections": {
			Name:   "MongoDB collections",
			Value:  "All required collections exist",
			Passed: true,
		},
		"elasticsearch": {
			Name:   "Elastic search is running",
			Value:  "Elastic search is not configured",
			Passed: true,
		},
		"entities": {
			Name:   "Entities in charm store",
			Value:  "4 charms; 2 bundles; 4 promulgated",
			Passed: true,
		},
		"base_entities": {
			Name:   "Base entities in charm store",
			Value:  "count: 5",
			Passed: true,
		},
		"server_started": {
			Name:   "Server started",
			Value:  now.String(),
			Passed: true,
		},
		"ingestion": {
			Name:   "Ingestion",
			Value:  "started: " + start.Format(time.RFC3339) + ", completed: " + end.Format(time.RFC3339),
			Passed: true,
		},
		"legacy_statistics": {
			Name:   "Legacy Statistics Load",
			Value:  "started: " + statisticsStart.Format(time.RFC3339) + ", completed: " + statisticsEnd.Format(time.RFC3339),
			Passed: true,
		},
	})
}

func addLog(c *gc.C, store *charmstore.Store, log *mongodoc.Log) {
	err := store.DB.Logs().Insert(log)
	c.Assert(err, gc.IsNil)
}

func (s *debugSuite) TestStatusWithoutCorrectCollections(c *gc.C) {
	store := s.srv.Pool().Store()
	defer store.Close()
	store.DB.Entities().DropCollection()
	s.AssertDebugStatus(c, false, map[string]params.DebugStatus{
		"mongo_collections": {
			Name:   "MongoDB collections",
			Value:  "Missing collections: [" + store.DB.Entities().Name + "]",
			Passed: false,
		},
	})
}

func (s *debugSuite) TestStatusWithoutIngestion(c *gc.C) {
	s.AssertDebugStatus(c, false, map[string]params.DebugStatus{
		"ingestion": {
			Name:   "Ingestion",
			Value:  "started: " + zeroTimeStr + ", completed: " + zeroTimeStr,
			Passed: false,
		},
	})
}

func (s *debugSuite) TestStatusIngestionStarted(c *gc.C) {
	store := s.srv.Pool().Store()
	defer store.Close()
	now := time.Now()
	start := now.Add(-1 * time.Hour)
	addLog(c, store, &mongodoc.Log{
		Data:  []byte(`"ingestion started"`),
		Level: mongodoc.InfoLevel,
		Type:  mongodoc.IngestionType,
		Time:  start,
	})
	s.AssertDebugStatus(c, false, map[string]params.DebugStatus{
		"ingestion": {
			Name:   "Ingestion",
			Value:  "started: " + start.Format(time.RFC3339) + ", completed: " + zeroTimeStr,
			Passed: false,
		},
	})
}

func (s *debugSuite) TestStatusWithoutLegacyStatistics(c *gc.C) {
	s.AssertDebugStatus(c, false, map[string]params.DebugStatus{
		"legacy_statistics": {
			Name:   "Legacy Statistics Load",
			Value:  "started: " + zeroTimeStr + ", completed: " + zeroTimeStr,
			Passed: false,
		},
	})
}

func (s *debugSuite) TestStatusLegacyStatisticsStarted(c *gc.C) {
	store := s.srv.Pool().Store()
	defer store.Close()
	now := time.Now()
	statisticsStart := now.Add(-1*time.Hour - 30*time.Minute)
	addLog(c, store, &mongodoc.Log{
		Data:  []byte(`"legacy statistics import started"`),
		Level: mongodoc.InfoLevel,
		Type:  mongodoc.LegacyStatisticsType,
		Time:  statisticsStart,
	})
	s.AssertDebugStatus(c, false, map[string]params.DebugStatus{
		"legacy_statistics": {
			Name:   "Legacy Statistics Load",
			Value:  "started: " + statisticsStart.Format(time.RFC3339) + ", completed: " + zeroTimeStr,
			Passed: false,
		},
	})
}

func (s *debugSuite) TestStatusLegacyStatisticsMultipleLogs(c *gc.C) {
	store := s.srv.Pool().Store()
	defer store.Close()
	now := time.Now()
	statisticsStart := now.Add(-1*time.Hour - 30*time.Minute)
	addLog(c, store, &mongodoc.Log{
		Data:  []byte(`"legacy statistics import started"`),
		Level: mongodoc.InfoLevel,
		Type:  mongodoc.LegacyStatisticsType,
		Time:  statisticsStart.Add(-1 * time.Hour),
	})
	addLog(c, store, &mongodoc.Log{
		Data:  []byte(`"legacy statistics import started"`),
		Level: mongodoc.InfoLevel,
		Type:  mongodoc.LegacyStatisticsType,
		Time:  statisticsStart,
	})
	statisticsEnd := now.Add(-30 * time.Minute)
	addLog(c, store, &mongodoc.Log{
		Data:  []byte(`"legacy statistics import completed"`),
		Level: mongodoc.InfoLevel,
		Type:  mongodoc.LegacyStatisticsType,
		Time:  statisticsEnd.Add(-1 * time.Hour),
	})
	addLog(c, store, &mongodoc.Log{
		Data:  []byte(`"legacy statistics import completed"`),
		Level: mongodoc.InfoLevel,
		Type:  mongodoc.LegacyStatisticsType,
		Time:  statisticsEnd,
	})
	s.AssertDebugStatus(c, false, map[string]params.DebugStatus{
		"legacy_statistics": {
			Name:   "Legacy Statistics Load",
			Value:  "started: " + statisticsStart.Format(time.RFC3339) + ", completed: " + statisticsEnd.Format(time.RFC3339),
			Passed: true,
		},
	})
}

func (s *debugSuite) TestStatusBaseEntitiesError(c *gc.C) {
	store := s.srv.Pool().Store()
	defer store.Close()
	// Add a base entity without any corresponding entities.
	entity := &mongodoc.BaseEntity{
		URL:  charm.MustParseURL("django"),
		Name: "django",
	}
	err := store.DB.BaseEntities().Insert(entity)
	c.Assert(err, gc.IsNil)

	s.AssertDebugStatus(c, false, map[string]params.DebugStatus{
		"base_entities": {
			Name:   "Base entities in charm store",
			Value:  "count: 1",
			Passed: false,
		},
	})
}

func (s *debugSuite) TestDebugCheck(c *gc.C) {
	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler: s.srv,
		URL:     "/debug/check",
		ExpectBody: map[string]string{
			"elasticsearch": "OK",
			"mongodb":       "OK",
		},
	})
}

func (s *debugSuite) TestDebugFullCheckUnauthorized(c *gc.C) {
	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler:      s.srv,
		URL:          "/debug/fullcheck",
		ExpectStatus: http.StatusUnauthorized,
		ExpectBody: params.Error{
			Message: "unauthorized: invalid or missing HTTP auth header",
			Code:    params.ErrUnauthorized,
		},
	})
}

// AssertDebugStatus asserts that the current /debug/status endpoint
// matches the given status, ignoring status duration.
// If complete is true, it fails if the results contain
// keys not mentioned in status.
func (s *debugSuite) AssertDebugStatus(c *gc.C, complete bool, status map[string]params.DebugStatus) {
	rec := httptesting.DoRequest(c, httptesting.DoRequestParams{
		Handler: s.srv,
		URL:     "/debug/status",
	})
	c.Assert(rec.Code, gc.Equals, http.StatusOK, gc.Commentf("body: %s", rec.Body.Bytes()))
	c.Assert(rec.Header().Get("Content-Type"), gc.Equals, "application/json")
	var gotStatus map[string]params.DebugStatus
	err := json.Unmarshal(rec.Body.Bytes(), &gotStatus)
	c.Assert(err, gc.IsNil)
	for key, r := range gotStatus {
		if _, found := status[key]; !complete && !found {
			delete(gotStatus, key)
			continue
		}
		r.Duration = 0
		gotStatus[key] = r
	}
	c.Assert(gotStatus, jc.DeepEquals, status)
}

func addEntitiesToStore(c *gc.C, store *charmstore.Store) {
	for _, id := range []*router.ResolvedURL{
		newResolvedURL("cs:~charmers/precise/wordpress-2", 2),
		newResolvedURL("cs:~charmers/precise/wordpress-3", 3),
		newResolvedURL("cs:~foo/precise/mysql-9", 1),
		newResolvedURL("cs:~bar/utopic/mysql-10", -1),
		newResolvedURL("cs:~charmers/bundle/wordpress-simple-3", 3),
		newResolvedURL("cs:~bar/bundle/wordpress-simple-4", -1),
	} {
		if id.URL.Series == "bundle" {
			err := store.AddBundleWithArchive(id, storetesting.NewBundle(&charm.BundleData{
				Services: map[string]*charm.ServiceSpec{
					"wordpress": &charm.ServiceSpec{
						Charm: "wordpress",
					},
				},
			}))
			c.Assert(err, gc.IsNil)
			err = store.SetPerms(&id.URL, "stable.read", params.Everyone)
			c.Assert(err, gc.IsNil)
			err = store.Publish(id, params.StableChannel)
			c.Assert(err, gc.IsNil)
		} else {
			err := store.AddCharmWithArchive(id, storetesting.NewCharm(nil))
			c.Assert(err, gc.IsNil)
			err = store.SetPerms(&id.URL, "stable.read", params.Everyone)
			c.Assert(err, gc.IsNil)
			err = store.Publish(id, params.StableChannel)
			c.Assert(err, gc.IsNil)
		}
	}
}

type debugWithElasticSearchSuite struct {
	storetesting.IsolatedMgoESSuite
	srv *charmstore.Server
	si  charmstore.SearchIndex
}

var _ = gc.Suite(&debugWithElasticSearchSuite{})

func (s *debugWithElasticSearchSuite) SetUpTest(c *gc.C) {
	s.IsolatedMgoESSuite.SetUpTest(c)
	var err error
	s.si = charmstore.SearchIndex{s.ES, s.TestIndex}
	s.srv, err = charmstore.NewServer(
		s.Session.DB("debug-testing"),
		&s.si,
		serverParams,
		map[string]charmstore.NewAPIHandlerFunc{
			"v4": v4.NewAPIHandler,
			"v5": v5.NewAPIHandler,
		},
	)
	c.Assert(err, gc.IsNil)
}

func (s *debugWithElasticSearchSuite) TearDownTest(c *gc.C) {
	s.srv.Close()
	s.IsolatedMgoESSuite.TearDownTest(c)
}

func (s *debugWithElasticSearchSuite) TestStatusWithElasticSearch(c *gc.C) {
	rec := httptesting.DoRequest(c, httptesting.DoRequestParams{
		Handler: s.srv,
		URL:     "/debug/status",
	})
	var results map[string]params.DebugStatus
	err := json.Unmarshal(rec.Body.Bytes(), &results)
	c.Assert(err, gc.IsNil)
	c.Assert(results["elasticsearch"].Name, gc.Equals, "Elastic search is running")
	c.Assert(results["elasticsearch"].Value, jc.Contains, "cluster_name:")
}

func (s *debugWithElasticSearchSuite) TestDebugCheckCheckers(c *gc.C) {
	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler: s.srv,
		URL:     "/debug/check",
		ExpectBody: map[string]string{
			"elasticsearch": "OK",
			"mongodb":       "OK",
		},
	})
}

func (s *debugWithElasticSearchSuite) TestDebugFullCheck(c *gc.C) {
	store := s.srv.Pool().Store()
	defer store.Close()
	addEntitiesToStore(c, store)
	s.ES.RefreshIndex(s.TestIndex)
	rec := httptesting.DoRequest(c, httptesting.DoRequestParams{
		Handler:  s.srv,
		URL:      "/debug/fullcheck",
		Username: serverParams.AuthUsername,
		Password: serverParams.AuthPassword,
	})
	c.Assert(rec.Code, gc.Equals, http.StatusOK, gc.Commentf("Body: %s", rec.Body))
}

var debugPprofTests = []struct {
	path  string
	match string
}{{
	path:  "/debug/pprof/",
	match: `(?s).*profiles:.*heap.*`,
}, {
	path:  "/debug/pprof/goroutine?debug=2",
	match: "(?s)goroutine [0-9]+.*",
}, {
	path:  "/debug/pprof/cmdline",
	match: ".+charmstore.+",
}}

func (s *debugWithElasticSearchSuite) TestDebugPprof(c *gc.C) {
	for i, test := range debugPprofTests {
		c.Logf("test %d: %s", i, test.path)

		rec := httptesting.DoRequest(c, httptesting.DoRequestParams{
			Handler:  s.srv,
			Username: serverParams.AuthUsername,
			Password: serverParams.AuthPassword,
			URL:      test.path,
		})
		c.Assert(rec.Code, gc.Equals, http.StatusOK, gc.Commentf("body: %s", rec.Body.String()))
		c.Assert(rec.Body.String(), gc.Matches, test.match)
	}
}

func (s *debugWithElasticSearchSuite) TestDebugPprofFailsWithoutAuth(c *gc.C) {
	for i, test := range debugPprofTests {
		c.Logf("test %d: %s", i, test.path)
		httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
			Handler:      s.srv,
			URL:          test.path,
			ExpectStatus: http.StatusUnauthorized,
			ExpectBody: params.Error{
				Message: "unauthorized: invalid or missing HTTP auth header",
				Code:    params.ErrUnauthorized,
			},
		})
	}
}
