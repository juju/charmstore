// Copyright 2014 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package v4_test

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/juju/charmrepo/v6/csclient/params"
	jc "github.com/juju/testing/checkers"
	"github.com/juju/testing/httptesting"
	"github.com/juju/utils/debugstatus"
	gc "gopkg.in/check.v1"

	"gopkg.in/juju/charmstore.v5/internal/charm"
	"gopkg.in/juju/charmstore.v5/internal/mongodoc"
	"gopkg.in/juju/charmstore.v5/internal/router"
)

var zeroTimeStr = time.Time{}.Format(time.RFC3339)

func (s *APISuite) TestStatus(c *gc.C) {
	for _, id := range []*router.ResolvedURL{
		newResolvedURL("cs:~charmers/precise/wordpress-2", 2),
		newResolvedURL("cs:~charmers/precise/wordpress-3", 3),
		newResolvedURL("cs:~foo/precise/mysql-9", 1),
		newResolvedURL("cs:~bar/utopic/mysql-10", -1),
		newResolvedURL("cs:~charmers/bundle/wordpress-simple-3", 3),
		newResolvedURL("cs:~bar/bundle/wordpress-simple-4", -1),
	} {
		if id.URL.Series == "bundle" {
			s.addPublicBundleFromRepo(c, id.URL.Name, id, false)
		} else {
			s.addPublicCharmFromRepo(c, id.URL.Name, id)
		}
	}
	now := time.Now()
	s.PatchValue(&debugstatus.StartTime, now)
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
	})
}

func (s *APISuite) TestStatusWithoutCorrectCollections(c *gc.C) {
	s.store.DB.Entities().DropCollection()
	s.AssertDebugStatus(c, false, map[string]params.DebugStatus{
		"mongo_collections": {
			Name:   "MongoDB collections",
			Value:  "Missing collections: [" + s.store.DB.Entities().Name + "]",
			Passed: false,
		},
	})
}

func (s *APISuite) TestStatusBaseEntitiesError(c *gc.C) {
	// Add a base entity without any corresponding entities.
	entity := &mongodoc.BaseEntity{
		URL:  charm.MustParseURL("django"),
		Name: "django",
	}
	err := s.store.DB.BaseEntities().Insert(entity)
	c.Assert(err, gc.Equals, nil)

	s.AssertDebugStatus(c, false, map[string]params.DebugStatus{
		"base_entities": {
			Name:   "Base entities in charm store",
			Value:  "count: 1",
			Passed: false,
		},
	})
}

// AssertDebugStatus asserts that the current /debug/status endpoint
// matches the given status, ignoring status duration.
// If complete is true, it fails if the results contain
// keys not mentioned in status.
func (s *APISuite) AssertDebugStatus(c *gc.C, complete bool, status map[string]params.DebugStatus) {
	rec := httptesting.DoRequest(c, httptesting.DoRequestParams{
		Handler: s.srv,
		URL:     storeURL("debug/status"),
	})
	c.Assert(rec.Code, gc.Equals, http.StatusOK, gc.Commentf("body: %s", rec.Body.Bytes()))
	c.Assert(rec.Header().Get("Content-Type"), gc.Equals, "application/json")
	var gotStatus map[string]params.DebugStatus
	err := json.Unmarshal(rec.Body.Bytes(), &gotStatus)
	c.Assert(err, gc.Equals, nil)
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

type statusWithElasticSearchSuite struct {
	commonSuite
}

var _ = gc.Suite(&statusWithElasticSearchSuite{})

func (s *statusWithElasticSearchSuite) SetUpSuite(c *gc.C) {
	s.enableES = true
	s.commonSuite.SetUpSuite(c)
}

func (s *statusWithElasticSearchSuite) TestStatusWithElasticSearch(c *gc.C) {
	rec := httptesting.DoRequest(c, httptesting.DoRequestParams{
		Handler: s.srv,
		URL:     storeURL("debug/status"),
	})
	var results map[string]params.DebugStatus
	err := json.Unmarshal(rec.Body.Bytes(), &results)
	c.Assert(err, gc.Equals, nil)
	c.Assert(results["elasticsearch"].Name, gc.Equals, "Elastic search is running")
	c.Assert(results["elasticsearch"].Value, jc.Contains, "cluster_name:")
}
