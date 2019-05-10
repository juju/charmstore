// Copyright 2014 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package v4_test

import (
	"encoding/json"
	"net/http"
	"sort"

	"github.com/juju/loggo"
	jc "github.com/juju/testing/checkers"
	"github.com/juju/testing/httptesting"
	gc "gopkg.in/check.v1"
	"gopkg.in/juju/charm.v6"
	"gopkg.in/juju/charmrepo.v3/csclient/params"
	"gopkg.in/mgo.v2/bson"

	"gopkg.in/juju/charmstore.v5/internal/router"
	"gopkg.in/juju/charmstore.v5/internal/storetesting"
)

type SearchSuite struct {
	commonSuite
}

var _ = gc.Suite(&SearchSuite{})

func (s *SearchSuite) SetUpSuite(c *gc.C) {
	s.enableES = true
	s.enableIdentity = true
	s.commonSuite.SetUpSuite(c)
}

func (s *SearchSuite) SetUpTest(c *gc.C) {
	s.commonSuite.SetUpTest(c)
	s.addCharmsToStore(c)
	err := s.store.SetPerms(charm.MustParseURL("cs:~charmers/riak"), "stable.read", "charmers", "test-user")
	c.Assert(err, gc.Equals, nil)
	err = s.store.UpdateSearch(newResolvedURL("~charmers/trusty/riak-0", 0))
	c.Assert(err, gc.Equals, nil)
	err = s.esSuite.ES.RefreshIndex(s.esSuite.TestIndex)
	c.Assert(err, gc.Equals, nil)
}

func (s *SearchSuite) addCharmsToStore(c *gc.C) {
	for _, e := range storetesting.SearchEntities {
		if e.Bundle != nil {
			continue
		}
		s.addPublicCharm(c, e.Charm, e.ResolvedURL())
	}
	for _, e := range storetesting.SearchEntities {
		if e.Bundle == nil {
			continue
		}
		s.addPublicBundle(c, e.Bundle, e.ResolvedURL(), false)
	}
}

func (s *SearchSuite) TestSuccessfulSearches(c *gc.C) {
	tests := []struct {
		about   string
		query   string
		results []*router.ResolvedURL
	}{{
		about: "bare search",
		query: "limit=20",
		results: storetesting.ResolvedURLs(
			[]storetesting.SearchEntity{
				storetesting.SearchEntities["multi-series"],
				storetesting.SearchEntities["wordpress"],
				storetesting.SearchEntities["mysql"],
				storetesting.SearchEntities["varnish"],
				storetesting.SearchEntities["wordpress-simple"],
				storetesting.SearchEntities["cloud-controller-worker-v2"],
				storetesting.SearchEntities["squid-forwardproxy"],
			},
			// V4 SPECIFIC
			true,
		),
	}, {
		about: "text search",
		query: "text=wordpress",
		results: storetesting.ResolvedURLs(
			[]storetesting.SearchEntity{
				storetesting.SearchEntities["wordpress"],
				storetesting.SearchEntities["wordpress-simple"],
			},
			// V4 SPECIFIC
			true,
		),
	}, {
		about: "autocomplete search",
		query: "text=word&autocomplete=1",
		results: storetesting.ResolvedURLs(
			[]storetesting.SearchEntity{
				storetesting.SearchEntities["wordpress"],
				storetesting.SearchEntities["wordpress-simple"],
			},
			// V4 SPECIFIC
			true,
		),
	}, {
		about: "blank text search",
		query: "text=&limit=20",
		results: storetesting.ResolvedURLs(
			[]storetesting.SearchEntity{
				storetesting.SearchEntities["multi-series"],
				storetesting.SearchEntities["wordpress"],
				storetesting.SearchEntities["mysql"],
				storetesting.SearchEntities["varnish"],
				storetesting.SearchEntities["wordpress-simple"],
				storetesting.SearchEntities["cloud-controller-worker-v2"],
				storetesting.SearchEntities["squid-forwardproxy"],
			},
			// V4 SPECIFIC
			true,
		),
	}, {
		about: "description filter search",
		query: "description=database",
		results: storetesting.ResolvedURLs(
			[]storetesting.SearchEntity{
				storetesting.SearchEntities["mysql"],
				storetesting.SearchEntities["varnish"],
			},
			// V4 SPECIFIC
			true,
		),
	}, {
		about: "name filter search",
		query: "name=mysql",
		results: storetesting.ResolvedURLs(
			[]storetesting.SearchEntity{
				storetesting.SearchEntities["mysql"],
			},
			// V4 SPECIFIC
			true,
		),
	}, {
		about: "owner filter search",
		query: "owner=foo",
		results: storetesting.ResolvedURLs(
			[]storetesting.SearchEntity{
				storetesting.SearchEntities["varnish"],
			},
			// V4 SPECIFIC
			true,
		),
	}, {
		about: "provides filter search",
		query: "provides=mysql",
		results: storetesting.ResolvedURLs(
			[]storetesting.SearchEntity{
				storetesting.SearchEntities["mysql"],
			},
			// V4 SPECIFIC
			true,
		),
	}, {
		about: "requires filter search",
		query: "requires=mysql",
		results: storetesting.ResolvedURLs(
			[]storetesting.SearchEntity{
				storetesting.SearchEntities["multi-series"],
				storetesting.SearchEntities["wordpress"],
			},
			// V4 SPECIFIC
			true,
		),
	}, {
		about: "series filter search",
		query: "series=" + storetesting.SearchSeries[0],
		results: []*router.ResolvedURL{
			// V4 SPECIFIC
			storetesting.ResolvedURLWithSeries(storetesting.SearchEntities["multi-series"].ResolvedURL(), storetesting.SearchSeries[0]),
			storetesting.SearchEntities["wordpress"].ResolvedURL(),
		},
	}, {
		about: "summary filter search",
		query: "summary=database",
		results: storetesting.ResolvedURLs(
			[]storetesting.SearchEntity{
				storetesting.SearchEntities["mysql"],
				storetesting.SearchEntities["varnish"],
			},
			// V4 SPECIFIC
			true,
		),
	}, {
		about: "tags filter search",
		query: "tags=wordpress",
		results: storetesting.ResolvedURLs(
			[]storetesting.SearchEntity{
				storetesting.SearchEntities["wordpress"],
				storetesting.SearchEntities["wordpress-simple"],
			},
			// V4 SPECIFIC
			true,
		),
	}, {
		about: "type filter search",
		query: "type=bundle",
		results: storetesting.ResolvedURLs(
			[]storetesting.SearchEntity{
				storetesting.SearchEntities["wordpress-simple"],
			},
			// V4 SPECIFIC
			true,
		),
	}, {
		about: "multiple type filter search",
		query: "type=bundle&type=charm&limit=20",
		results: storetesting.ResolvedURLs(
			[]storetesting.SearchEntity{
				storetesting.SearchEntities["multi-series"],
				storetesting.SearchEntities["wordpress"],
				storetesting.SearchEntities["mysql"],
				storetesting.SearchEntities["varnish"],
				storetesting.SearchEntities["wordpress-simple"],
				storetesting.SearchEntities["cloud-controller-worker-v2"],
				storetesting.SearchEntities["squid-forwardproxy"],
			},
			// V4 SPECIFIC
			true,
		),
	}, {
		about: "provides multiple interfaces filter search",
		query: "provides=monitoring+http",
		results: storetesting.ResolvedURLs(
			[]storetesting.SearchEntity{
				storetesting.SearchEntities["multi-series"],
				storetesting.SearchEntities["wordpress"],
			},
			// V4 SPECIFIC
			true,
		),
	}, {
		about: "requires multiple interfaces filter search",
		query: "requires=mysql+varnish",
		results: storetesting.ResolvedURLs(
			[]storetesting.SearchEntity{
				storetesting.SearchEntities["multi-series"],
				storetesting.SearchEntities["wordpress"],
			},
			// V4 SPECIFIC
			true,
		),
	}, {
		about: "multiple tags filter search",
		query: "tags=mysql+bar",
		results: storetesting.ResolvedURLs(
			[]storetesting.SearchEntity{
				storetesting.SearchEntities["mysql"],
			},
			// V4 SPECIFIC
			true,
		),
	}, {
		about: "blank owner",
		query: "owner=",
		results: storetesting.ResolvedURLs(
			[]storetesting.SearchEntity{
				storetesting.SearchEntities["multi-series"],
				storetesting.SearchEntities["wordpress"],
				storetesting.SearchEntities["mysql"],
				storetesting.SearchEntities["wordpress-simple"],
				storetesting.SearchEntities["squid-forwardproxy"],
			},
			// V4 SPECIFIC
			true,
		),
	}, {
		about: "paginated search",
		query: "name=mysql&skip=1",
	}, {
		about: "promulgated",
		query: "promulgated=1",
		results: storetesting.ResolvedURLs(
			[]storetesting.SearchEntity{
				storetesting.SearchEntities["multi-series"],
				storetesting.SearchEntities["wordpress"],
				storetesting.SearchEntities["mysql"],
				storetesting.SearchEntities["wordpress-simple"],
				storetesting.SearchEntities["squid-forwardproxy"],
			},
			// V4 SPECIFIC
			true,
		),
	}, {
		about: "not promulgated",
		query: "promulgated=0",
		results: storetesting.ResolvedURLs(
			[]storetesting.SearchEntity{
				storetesting.SearchEntities["varnish"],
				storetesting.SearchEntities["cloud-controller-worker-v2"],
			},
			// V4 SPECIFIC
			true,
		),
	}, {
		about: "promulgated with owner",
		query: "promulgated=1&owner=openstack-charmers",
		results: storetesting.ResolvedURLs(
			[]storetesting.SearchEntity{
				storetesting.SearchEntities["mysql"],
			},
			// V4 SPECIFIC
			true,
		),
	}}
	for i, test := range tests {
		c.Logf("test %d. %s", i, test.about)
		rec := httptesting.DoRequest(c, httptesting.DoRequestParams{
			Handler: s.srv,
			URL:     storeURL("search?" + test.query),
		})
		var sr params.SearchResponse
		err := json.Unmarshal(rec.Body.Bytes(), &sr)
		c.Assert(err, gc.Equals, nil)
		c.Assert(sr.Results, gc.HasLen, len(test.results))
		c.Logf("results: %s", rec.Body.Bytes())
		assertResultSet(c, sr, test.results)
	}
}

func (s *SearchSuite) TestPaginatedSearch(c *gc.C) {
	rec := httptesting.DoRequest(c, httptesting.DoRequestParams{
		Handler: s.srv,
		URL:     storeURL("search?text=wordpress&skip=1"),
	})
	var sr params.SearchResponse
	err := json.Unmarshal(rec.Body.Bytes(), &sr)
	c.Assert(err, gc.Equals, nil)
	c.Assert(sr.Results, gc.HasLen, 1)
	c.Assert(sr.Total, gc.Equals, 2)
}

func (s *SearchSuite) TestMetadataFields(c *gc.C) {
	tests := []struct {
		about string
		query string
		meta  map[string]interface{}
	}{{
		about: "archive-size",
		query: "name=mysql&include=archive-size",
		meta: map[string]interface{}{
			"archive-size": params.ArchiveSizeResponse{storetesting.SearchEntities["mysql"].Charm.Size()},
		},
	}, {
		about: "bundle-metadata",
		query: "name=wordpress-simple&type=bundle&include=bundle-metadata",
		meta: map[string]interface{}{
			"bundle-metadata": v4BundleMetadata(storetesting.SearchEntities["wordpress-simple"].Bundle.Data()), // V4 SPECIFIC
		},
	}, {
		about: "bundle-machine-count",
		query: "name=wordpress-simple&type=bundle&include=bundle-machine-count",
		meta: map[string]interface{}{
			"bundle-machine-count": params.BundleCount{1},
		},
	}, {
		about: "bundle-unit-count",
		query: "name=wordpress-simple&type=bundle&include=bundle-unit-count",
		meta: map[string]interface{}{
			"bundle-unit-count": params.BundleCount{1},
		},
	}, {
		about: "charm-actions",
		query: "name=wordpress&type=charm&include=charm-actions",
		meta: map[string]interface{}{
			"charm-actions": storetesting.SearchEntities["wordpress"].Charm.Actions(),
		},
	}, {
		about: "charm-config",
		query: "name=wordpress&type=charm&include=charm-config",
		meta: map[string]interface{}{
			"charm-config": storetesting.SearchEntities["wordpress"].Charm.Config(),
		},
	}, {
		about: "charm-related",
		query: "name=wordpress&type=charm&include=charm-related",
		meta: map[string]interface{}{
			"charm-related": params.RelatedResponse{
				Provides: map[string][]params.EntityResult{
					"mysql": {
						{
							Id: storetesting.SearchEntities["mysql"].ResolvedURL().PreferredURL(),
						},
					},
				},
			},
		},
	}, {
		about: "multiple values",
		query: "name=wordpress&type=charm&include=charm-related&include=charm-config",
		meta: map[string]interface{}{
			"charm-related": params.RelatedResponse{
				Provides: map[string][]params.EntityResult{
					"mysql": {
						{
							Id: storetesting.SearchEntities["mysql"].ResolvedURL().PreferredURL(),
						},
					},
				},
			},
			"charm-config": storetesting.SearchEntities["worpress"].Charm.Config(),
		},
	}}
	for i, test := range tests {
		c.Logf("test %d. %s", i, test.about)
		rec := httptesting.DoRequest(c, httptesting.DoRequestParams{
			Handler: s.srv,
			URL:     storeURL("search?" + test.query),
		})
		c.Assert(rec.Code, gc.Equals, http.StatusOK)
		var sr struct {
			Results []struct {
				Meta json.RawMessage
			}
		}
		err := json.Unmarshal(rec.Body.Bytes(), &sr)
		c.Assert(err, gc.Equals, nil)
		c.Assert(sr.Results, gc.HasLen, 1)
		c.Assert(string(sr.Results[0].Meta), jc.JSONEquals, test.meta)
	}
}

func (s *SearchSuite) TestSearchError(c *gc.C) {
	err := s.esSuite.ES.DeleteIndex(s.esSuite.TestIndex)
	c.Assert(err, gc.Equals, nil)
	rec := httptesting.DoRequest(c, httptesting.DoRequestParams{
		Handler: s.srv,
		URL:     storeURL("search?name=wordpress"),
	})
	c.Assert(rec.Code, gc.Equals, http.StatusInternalServerError)
	var resp params.Error
	err = json.Unmarshal(rec.Body.Bytes(), &resp)
	c.Assert(err, gc.Equals, nil)
	c.Assert(resp.Code, gc.Equals, params.ErrorCode(""))
	c.Assert(resp.Message, gc.Matches, "error performing search: search failed: .*")
}

func (s *SearchSuite) TestSearchIncludeError(c *gc.C) {
	// Perform a search for all charms, including the
	// manifest, which will try to retrieve all charm
	// blobs.
	rec := httptesting.DoRequest(c, httptesting.DoRequestParams{
		Handler: s.srv,
		URL:     storeURL("search?type=charm&include=manifest"),
	})
	c.Assert(rec.Code, gc.Equals, http.StatusOK)
	var resp params.SearchResponse
	err := json.Unmarshal(rec.Body.Bytes(), &resp)
	// V4 SPECIFIC
	// cs:riak will not be found because it is not visible to "everyone".
	// cs:multi-series will be expanded to 4 different results.
	// cs:wordpress-simple won't be found as it is a bundle
	c.Assert(resp.Results, gc.HasLen, len(storetesting.SearchEntities)+3-2)

	// Now update the entity to hold an invalid hash.
	// The list should still work, but only return a single result.
	rurl := storetesting.SearchEntities["wordpress"].ResolvedURL()
	err = s.store.UpdateEntity(rurl, bson.D{{
		"$set", bson.D{{
			"blobhash", hashOfString("nope"),
		}},
	}})
	c.Assert(err, gc.Equals, nil)
	err = s.store.UpdateSearch(rurl)
	c.Assert(err, gc.Equals, nil)
	err = s.esSuite.ES.RefreshIndex(s.esSuite.TestIndex)
	c.Assert(err, gc.Equals, nil)

	// Now search again - we should get one result less
	// (and the error will be logged).

	// Register a logger that so that we can check the logging output.
	// It will be automatically removed later because IsolatedMgoESSuite
	// uses LoggingSuite.
	var tw loggo.TestWriter
	err = loggo.RegisterWriter("test-log", &tw)
	c.Assert(err, gc.Equals, nil)

	rec = httptesting.DoRequest(c, httptesting.DoRequestParams{
		Handler: s.srv,
		URL:     storeURL("search?type=charm&include=manifest"),
	})
	c.Assert(rec.Code, gc.Equals, http.StatusOK)
	resp = params.SearchResponse{}
	err = json.Unmarshal(rec.Body.Bytes(), &resp)
	// V4 SPECIFIC
	// cs:riak will not be found because it is not visible to "everyone".
	// cs:multi-series will be expanded to a result for each series.
	// cs:wordpress will not be found because it has no manifest.
	// cs:wordpress-simple won't be found as it is a bundle
	c.Assert(resp.Results, gc.HasLen, len(storetesting.SearchEntities)+len(storetesting.SearchSeries)-4)

	c.Assert(tw.Log(), jc.LogMatches, []string{"cannot retrieve metadata for cs:" + storetesting.SearchSeries[0] + "/wordpress-23: cannot open archive data for cs:" + storetesting.SearchSeries[0] + "/wordpress-23: .*"})
}

func (s *SearchSuite) TestSorting(c *gc.C) {
	tests := []struct {
		about   string
		query   string
		results []*router.ResolvedURL
	}{{
		about: "name ascending",
		query: "sort=name&limit=20",
		results: storetesting.ResolvedURLs(
			[]storetesting.SearchEntity{
				storetesting.SearchEntities["cloud-controller-worker-v2"],
				storetesting.SearchEntities["multi-series"],
				storetesting.SearchEntities["mysql"],
				storetesting.SearchEntities["squid-forwardproxy"],
				storetesting.SearchEntities["varnish"],
				storetesting.SearchEntities["wordpress"],
				storetesting.SearchEntities["wordpress-simple"],
			},
			// V4 SPECIFIC
			true,
		),
	}, {
		about: "name descending",
		query: "sort=-name&limit=20",
		results: storetesting.ResolvedURLs(
			[]storetesting.SearchEntity{
				storetesting.SearchEntities["wordpress-simple"],
				storetesting.SearchEntities["wordpress"],
				storetesting.SearchEntities["varnish"],
				storetesting.SearchEntities["squid-forwardproxy"],
				storetesting.SearchEntities["mysql"],
				storetesting.SearchEntities["multi-series"],
				storetesting.SearchEntities["cloud-controller-worker-v2"],
			},
			// V4 SPECIFIC
			true,
		),
	}, {
		about: "series ascending",
		query: "sort=series,name&limit=20",
		results: storetesting.SortResolvedURLsBySeries(
			storetesting.ResolvedURLs(
				[]storetesting.SearchEntity{
					storetesting.SearchEntities["cloud-controller-worker-v2"],
					storetesting.SearchEntities["multi-series"],
					storetesting.SearchEntities["mysql"],
					storetesting.SearchEntities["squid-forwardproxy"],
					storetesting.SearchEntities["varnish"],
					storetesting.SearchEntities["wordpress"],
					storetesting.SearchEntities["wordpress-simple"],
				},
				// V4 SPECIFIC
				true),
			false,
		),
	}, {
		about: "series descending",
		query: "sort=-series&sort=name&limit=20",
		results: storetesting.SortResolvedURLsBySeries(
			storetesting.ResolvedURLs(
				[]storetesting.SearchEntity{
					storetesting.SearchEntities["cloud-controller-worker-v2"],
					storetesting.SearchEntities["multi-series"],
					storetesting.SearchEntities["mysql"],
					storetesting.SearchEntities["squid-forwardproxy"],
					storetesting.SearchEntities["varnish"],
					storetesting.SearchEntities["wordpress"],
					storetesting.SearchEntities["wordpress-simple"],
				},
				// V4 SPECIFIC
				true),
			true,
		),
	}, {
		about: "owner ascending",
		query: "sort=owner,name&limit=20",
		results: storetesting.ResolvedURLs(
			[]storetesting.SearchEntity{
				storetesting.SearchEntities["cloud-controller-worker-v2"],
				storetesting.SearchEntities["multi-series"],
				storetesting.SearchEntities["squid-forwardproxy"],
				storetesting.SearchEntities["wordpress"],
				storetesting.SearchEntities["wordpress-simple"],
				storetesting.SearchEntities["varnish"],
				storetesting.SearchEntities["mysql"],
			},
			// V4 SPECIFIC
			true,
		),
	}, {
		about: "owner descending",
		query: "sort=-owner&sort=name&limit=20",
		results: storetesting.ResolvedURLs(
			[]storetesting.SearchEntity{
				storetesting.SearchEntities["mysql"],
				storetesting.SearchEntities["varnish"],
				storetesting.SearchEntities["multi-series"],
				storetesting.SearchEntities["squid-forwardproxy"],
				storetesting.SearchEntities["wordpress"],
				storetesting.SearchEntities["wordpress-simple"],
				storetesting.SearchEntities["cloud-controller-worker-v2"],
			},
			// V4 SPECIFIC
			true,
		),
	}}
	for i, test := range tests {
		c.Logf("test %d. %s", i, test.about)
		rec := httptesting.DoRequest(c, httptesting.DoRequestParams{
			Handler: s.srv,
			URL:     storeURL("search?" + test.query),
		})
		var sr params.SearchResponse
		err := json.Unmarshal(rec.Body.Bytes(), &sr)
		c.Assert(err, gc.Equals, nil)
		// Not using assertResultSet(c, sr, test.results) as it does sort internally
		c.Assert(sr.Results, gc.HasLen, len(test.results), gc.Commentf("expected %#v", test.results))
		c.Logf("results: %s", rec.Body.Bytes())
		for i := range test.results {
			c.Assert(sr.Results[i].Id.String(), gc.Equals, test.results[i].PreferredURL().String(), gc.Commentf("element %d", i))
		}
	}
}

func (s *SearchSuite) TestSortUnsupportedField(c *gc.C) {
	rec := httptesting.DoRequest(c, httptesting.DoRequestParams{
		Handler: s.srv,
		URL:     storeURL("search?sort=foo"),
	})
	var e params.Error
	err := json.Unmarshal(rec.Body.Bytes(), &e)
	c.Assert(err, gc.Equals, nil)
	c.Assert(e.Code, gc.Equals, params.ErrBadRequest)
	c.Assert(e.Message, gc.Equals, "invalid sort field: unrecognized sort parameter \"foo\"")
}

func (s *SearchSuite) TestDownloadsBoost(c *gc.C) {
	charmDownloads := map[string]int{
		"mysql":     0,
		"wordpress": 1,
		"varnish":   8,
	}
	for n, cnt := range charmDownloads {
		url := newResolvedURL("cs:~downloads-test/"+storetesting.SearchSeries[1]+"/x-1", -1)
		url.URL.Name = n
		s.addPublicCharm(c, storetesting.SearchEntities[n].Charm, url)
		for i := 0; i < cnt; i++ {
			err := s.store.IncrementDownloadCounts(url)
			c.Assert(err, gc.Equals, nil)
		}
	}
	err := s.esSuite.ES.RefreshIndex(s.esSuite.TestIndex)
	c.Assert(err, gc.Equals, nil)
	rec := httptesting.DoRequest(c, httptesting.DoRequestParams{
		Handler: s.srv,
		URL:     storeURL("search?owner=downloads-test"),
	})
	var sr params.SearchResponse
	err = json.Unmarshal(rec.Body.Bytes(), &sr)
	c.Assert(err, gc.Equals, nil)
	c.Assert(sr.Results, gc.HasLen, 3)
	c.Assert(sr.Results[0].Id.Name, gc.Equals, "varnish")
	c.Assert(sr.Results[1].Id.Name, gc.Equals, "wordpress")
	c.Assert(sr.Results[2].Id.Name, gc.Equals, "mysql")
}

func (s *SearchSuite) TestSearchWithAdminCredentials(c *gc.C) {
	rec := httptesting.DoRequest(c, httptesting.DoRequestParams{
		Handler:  s.srv,
		URL:      storeURL("search?limit=2000"),
		Username: testUsername,
		Password: testPassword,
	})
	c.Assert(rec.Code, gc.Equals, http.StatusOK)
	expected := storetesting.ResolvedURLs(
		[]storetesting.SearchEntity{
			storetesting.SearchEntities["multi-series"],
			storetesting.SearchEntities["mysql"],
			storetesting.SearchEntities["wordpress"],
			storetesting.SearchEntities["riak"],
			storetesting.SearchEntities["varnish"],
			storetesting.SearchEntities["wordpress-simple"],
			storetesting.SearchEntities["cloud-controller-worker-v2"],
			storetesting.SearchEntities["squid-forwardproxy"],
		},
		// V4 SPECIFIC
		true,
	)
	var sr params.SearchResponse
	err := json.Unmarshal(rec.Body.Bytes(), &sr)
	c.Assert(err, gc.Equals, nil)
	assertResultSet(c, sr, expected)
}

func (s *SearchSuite) TestSearchWithUserMacaroon(c *gc.C) {
	rec := httptesting.DoRequest(c, httptesting.DoRequestParams{
		Handler: s.srv,
		URL:     storeURL("search?limit=2000"),
		Do:      bakeryDo(s.login("test-user")),
	})
	c.Assert(rec.Code, gc.Equals, http.StatusOK)
	expected := storetesting.ResolvedURLs(
		[]storetesting.SearchEntity{
			storetesting.SearchEntities["multi-series"],
			storetesting.SearchEntities["mysql"],
			storetesting.SearchEntities["wordpress"],
			storetesting.SearchEntities["riak"],
			storetesting.SearchEntities["varnish"],
			storetesting.SearchEntities["wordpress-simple"],
			storetesting.SearchEntities["cloud-controller-worker-v2"],
			storetesting.SearchEntities["squid-forwardproxy"],
		},
		// V4 SPECIFIC
		true,
	)

	var sr params.SearchResponse
	err := json.Unmarshal(rec.Body.Bytes(), &sr)
	c.Assert(err, gc.Equals, nil)
	assertResultSet(c, sr, expected)
}

func (s *SearchSuite) TestSearchWithUserInGroups(c *gc.C) {
	s.idmServer.AddUser("bob", "test-user", "test-user2")
	rec := httptesting.DoRequest(c, httptesting.DoRequestParams{
		Handler: s.srv,
		URL:     storeURL("search?limit=2000"),
		Do:      bakeryDo(s.login("bob")),
	})
	c.Assert(rec.Code, gc.Equals, http.StatusOK)
	expected := storetesting.ResolvedURLs(
		[]storetesting.SearchEntity{
			storetesting.SearchEntities["multi-series"],
			storetesting.SearchEntities["mysql"],
			storetesting.SearchEntities["wordpress"],
			storetesting.SearchEntities["riak"],
			storetesting.SearchEntities["varnish"],
			storetesting.SearchEntities["wordpress-simple"],
			storetesting.SearchEntities["cloud-controller-worker-v2"],
			storetesting.SearchEntities["squid-forwardproxy"],
		},
		// V4 SPECIFIC
		true,
	)
	var sr params.SearchResponse
	err := json.Unmarshal(rec.Body.Bytes(), &sr)
	c.Assert(err, gc.Equals, nil)
	assertResultSet(c, sr, expected)
}

func (s *SearchSuite) TestSearchWithBadAdminCredentialsAndACookie(c *gc.C) {
	rec := httptesting.DoRequest(c, httptesting.DoRequestParams{
		Handler:  s.srv,
		Do:       s.bakeryDoAsUser("test-user"),
		URL:      storeURL("search?limit=2000"),
		Username: testUsername,
		Password: "bad-password",
	})
	c.Assert(rec.Code, gc.Equals, http.StatusOK)
	expected := storetesting.ResolvedURLs(
		[]storetesting.SearchEntity{
			storetesting.SearchEntities["multi-series"],
			storetesting.SearchEntities["mysql"],
			storetesting.SearchEntities["wordpress"],
			storetesting.SearchEntities["varnish"],
			storetesting.SearchEntities["wordpress-simple"],
			storetesting.SearchEntities["cloud-controller-worker-v2"],
			storetesting.SearchEntities["squid-forwardproxy"],
		},
		// V4 SPECIFIC
		true,
	)
	var sr params.SearchResponse
	err := json.Unmarshal(rec.Body.Bytes(), &sr)
	c.Assert(err, gc.Equals, nil)
	assertResultSet(c, sr, expected)
}

func assertResultSet(c *gc.C, sr params.SearchResponse, expected []*router.ResolvedURL) {
	results := make([]string, len(sr.Results))
	for i, r := range sr.Results {
		results[i] = r.Id.String()
	}
	expect := make([]string, len(expected))
	for i, e := range expected {
		expect[i] = e.PreferredURL().String()
	}
	sort.Strings(results)
	sort.Strings(expect)
	c.Assert(results, jc.DeepEquals, expect)
}
