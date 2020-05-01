// Copyright 2014 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package v5_test

import (
	"encoding/json"
	"net/http"
	"net/url"
	"sort"

	"github.com/juju/charm/v7"
	"github.com/juju/charmrepo/v5/csclient/params"
	"github.com/juju/loggo"
	jc "github.com/juju/testing/checkers"
	"github.com/juju/testing/httptesting"
	gc "gopkg.in/check.v1"
	"gopkg.in/mgo.v2/bson"

	"gopkg.in/juju/charmstore.v5/internal/charmstore"
	"gopkg.in/juju/charmstore.v5/internal/router"
	"gopkg.in/juju/charmstore.v5/internal/storetesting"
	v5 "gopkg.in/juju/charmstore.v5/internal/v5"
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

func (s *SearchSuite) TestParseSearchParams(c *gc.C) {
	tests := []struct {
		about        string
		query        string
		expectParams charmstore.SearchParams
		expectError  string
	}{{
		about: "bare search",
		query: "",
		expectParams: charmstore.SearchParams{
			AutoComplete: true,
		},
	}, {
		about: "text search",
		query: "text=test&autocomplete=0",
		expectParams: charmstore.SearchParams{
			Text: "test",
		},
	}, {
		about: "autocomplete=0",
		query: "autocomplete=0",
		expectParams: charmstore.SearchParams{
			AutoComplete: false,
		},
	}, {
		about: "autocomplete=1",
		query: "autocomplete=1",
		expectParams: charmstore.SearchParams{
			AutoComplete: true,
		},
	}, {
		about:       "invalid autocomplete",
		query:       "autocomplete=true",
		expectError: `invalid autocomplete parameter: unexpected bool value "true" \(must be "0" or "1"\)`,
	}, {
		about: "limit",
		query: "limit=20&autocomplete=0",
		expectParams: charmstore.SearchParams{
			Limit: 20,
		},
	}, {
		about:       "invalid limit",
		query:       "limit=twenty&autocomplete=0",
		expectError: `invalid limit parameter: could not parse integer: strconv.(ParseInt|Atoi): parsing "twenty": invalid syntax`,
	}, {
		about:       "limit too low",
		query:       "limit=-1&autocomplete=0",
		expectError: "invalid limit parameter: expected integer greater than zero",
	}, {
		about: "include",
		query: "include=archive-size&autocomplete=0",
		expectParams: charmstore.SearchParams{
			Include: []string{"archive-size"},
		},
	}, {
		about: "include many",
		query: "include=archive-size&include=bundle-data&autocomplete=0",
		expectParams: charmstore.SearchParams{
			Include: []string{"archive-size", "bundle-data"},
		},
	}, {
		about: "include many with blanks",
		query: "include=archive-size&include=&include=bundle-data&autocomplete=0",
		expectParams: charmstore.SearchParams{
			Include: []string{"archive-size", "bundle-data"},
		},
	}, {
		about: "description filter",
		query: "description=text&autocomplete=0",
		expectParams: charmstore.SearchParams{
			Filters: map[string][]string{
				"description": {"text"},
			},
		},
	}, {
		about: "name filter",
		query: "name=text&autocomplete=0",
		expectParams: charmstore.SearchParams{
			Filters: map[string][]string{
				"name": {"text"},
			},
		},
	}, {
		about: "owner filter",
		query: "owner=text&autocomplete=0",
		expectParams: charmstore.SearchParams{
			Filters: map[string][]string{
				"owner": {"text"},
			},
		},
	}, {
		about: "provides filter",
		query: "provides=text&autocomplete=0",
		expectParams: charmstore.SearchParams{
			Filters: map[string][]string{
				"provides": {"text"},
			},
		},
	}, {
		about: "requires filter",
		query: "requires=text&autocomplete=0",
		expectParams: charmstore.SearchParams{
			Filters: map[string][]string{
				"requires": {"text"},
			},
		},
	}, {
		about: "series filter",
		query: "series=text&autocomplete=0",
		expectParams: charmstore.SearchParams{
			Filters: map[string][]string{
				"series": {"text"},
			},
		},
	}, {
		about: "tags filter",
		query: "tags=text&autocomplete=0",
		expectParams: charmstore.SearchParams{
			Filters: map[string][]string{
				"tags": {"text"},
			},
		},
	}, {
		about: "type filter",
		query: "type=text&autocomplete=0",
		expectParams: charmstore.SearchParams{
			Filters: map[string][]string{
				"type": {"text"},
			},
		},
	}, {
		about: "many filters",
		query: "name=name&owner=owner&series=series1&series=series2&autocomplete=0",
		expectParams: charmstore.SearchParams{
			Filters: map[string][]string{
				"name":   {"name"},
				"owner":  {"owner"},
				"series": {"series1", "series2"},
			},
		},
	}, {
		about:       "bad parameter",
		query:       "a=b",
		expectError: "invalid parameter: a",
	}, {
		about: "skip",
		query: "skip=20&autocomplete=0",
		expectParams: charmstore.SearchParams{
			Skip: 20,
		},
	}, {
		about:       "invalid skip",
		query:       "skip=twenty",
		expectError: `invalid skip parameter: could not parse integer: strconv.(ParseInt|Atoi): parsing "twenty": invalid syntax`,
	}, {
		about:       "skip too low",
		query:       "skip=-1",
		expectError: "invalid skip parameter: expected non-negative integer",
	}, {
		about: "promulgated filter",
		query: "promulgated=1&autocomplete=0",
		expectParams: charmstore.SearchParams{
			Filters: map[string][]string{
				"promulgated": {"1"},
			},
		},
	}, {
		about:       "promulgated filter - bad",
		query:       "promulgated=bad",
		expectError: `invalid promulgated filter parameter: unexpected bool value "bad" \(must be "0" or "1"\)`,
	}}
	for i, test := range tests {
		c.Logf("test %d. %s", i, test.about)
		var req http.Request
		var err error
		req.Form, err = url.ParseQuery(test.query)
		c.Assert(err, gc.Equals, nil)
		sp, err := v5.ParseSearchParams(&req)
		if test.expectError != "" {
			c.Assert(err, gc.ErrorMatches, test.expectError)
		} else {
			c.Assert(err, gc.Equals, nil)
		}
		c.Assert(sp, jc.DeepEquals, test.expectParams)
	}
}

func (s *SearchSuite) TestSuccessfulSearches(c *gc.C) {
	tests := []struct {
		about   string
		query   string
		results []*router.ResolvedURL
	}{{
		about: "bare search",
		query: "",
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
			false,
		),
	}, {
		about: "text search",
		query: "text=wordpress",
		results: storetesting.ResolvedURLs(
			[]storetesting.SearchEntity{
				storetesting.SearchEntities["wordpress"],
				storetesting.SearchEntities["wordpress-simple"],
			},
			false,
		),
	}, {
		about: "autocomplete search",
		query: "text=word&autocomplete=1",
		results: storetesting.ResolvedURLs(
			[]storetesting.SearchEntity{
				storetesting.SearchEntities["wordpress"],
				storetesting.SearchEntities["wordpress-simple"],
			},
			false,
		),
	}, {
		about: "blank text search",
		query: "text=",
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
			false,
		),
	}, {
		about: "description filter search",
		query: "description=database",
		results: storetesting.ResolvedURLs(
			[]storetesting.SearchEntity{
				storetesting.SearchEntities["mysql"],
				storetesting.SearchEntities["varnish"],
			},
			false,
		),
	}, {
		about: "name filter search",
		query: "name=mysql",
		results: storetesting.ResolvedURLs(
			[]storetesting.SearchEntity{
				storetesting.SearchEntities["mysql"],
			},
			false,
		),
	}, {
		about: "owner filter search",
		query: "owner=foo",
		results: storetesting.ResolvedURLs(
			[]storetesting.SearchEntity{
				storetesting.SearchEntities["varnish"],
			},
			false,
		),
	}, {
		about: "provides filter search",
		query: "provides=mysql",
		results: storetesting.ResolvedURLs(
			[]storetesting.SearchEntity{
				storetesting.SearchEntities["mysql"],
			},
			false,
		),
	}, {
		about: "requires filter search",
		query: "requires=mysql",
		results: storetesting.ResolvedURLs(
			[]storetesting.SearchEntity{
				storetesting.SearchEntities["multi-series"],
				storetesting.SearchEntities["wordpress"],
			},
			false,
		),
	}, {
		about: "series filter search",
		query: "series=" + storetesting.SearchSeries[0],
		results: storetesting.ResolvedURLs(
			[]storetesting.SearchEntity{
				storetesting.SearchEntities["multi-series"],
				storetesting.SearchEntities["wordpress"],
			},
			false,
		),
	}, {
		about: "summary filter search",
		query: "summary=database",
		results: storetesting.ResolvedURLs(
			[]storetesting.SearchEntity{
				storetesting.SearchEntities["mysql"],
				storetesting.SearchEntities["varnish"],
			},
			false,
		),
	}, {
		about: "tags filter search",
		query: "tags=wordpress",
		results: storetesting.ResolvedURLs(
			[]storetesting.SearchEntity{
				storetesting.SearchEntities["wordpress"],
				storetesting.SearchEntities["wordpress-simple"],
			},
			false,
		),
	}, {
		about: "type filter search",
		query: "type=bundle",
		results: storetesting.ResolvedURLs(
			[]storetesting.SearchEntity{
				storetesting.SearchEntities["wordpress-simple"],
			},
			false,
		),
	}, {
		about: "multiple type filter search",
		query: "type=bundle&type=charm",
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
			false,
		),
	}, {
		about: "provides multiple interfaces filter search",
		query: "provides=monitoring+http",
		results: storetesting.ResolvedURLs(
			[]storetesting.SearchEntity{
				storetesting.SearchEntities["multi-series"],
				storetesting.SearchEntities["wordpress"],
			},
			false,
		),
	}, {
		about: "requires multiple interfaces filter search",
		query: "requires=mysql+varnish",
		results: storetesting.ResolvedURLs(
			[]storetesting.SearchEntity{
				storetesting.SearchEntities["multi-series"],
				storetesting.SearchEntities["wordpress"],
			},
			false,
		),
	}, {
		about: "multiple tags filter search",
		query: "tags=mysql+bar",
		results: storetesting.ResolvedURLs(
			[]storetesting.SearchEntity{
				storetesting.SearchEntities["mysql"],
			},
			false,
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
			false,
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
			false,
		),
	}, {
		about: "not promulgated",
		query: "promulgated=0",
		results: storetesting.ResolvedURLs(
			[]storetesting.SearchEntity{
				storetesting.SearchEntities["varnish"],
				storetesting.SearchEntities["cloud-controller-worker-v2"],
			},
			false,
		),
	}, {
		about: "promulgated with owner",
		query: "promulgated=1&owner=openstack-charmers",
		results: storetesting.ResolvedURLs(
			[]storetesting.SearchEntity{
				storetesting.SearchEntities["mysql"],
			},
			false,
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
			"bundle-metadata": storetesting.SearchEntities["wordpress-simple"].Bundle.Data(),
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
			"charm-config": storetesting.SearchEntities["wordpress"].Charm.Config(),
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
	// cs:riak will not be found because it is not visible to "everyone".
	// cs:wordpress-simple will not be found as it is a bundle
	c.Assert(resp.Results, gc.HasLen, len(storetesting.SearchEntities)-2)

	// Now update the entity to hold an invalid hash.
	// The list should still work, but only return a single result.
	rurl := newResolvedURL("~charmers/"+storetesting.SearchSeries[0]+"/wordpress-23", 23)
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
	// cs:riak will not be found because it is not visible to "everyone".
	// cs:wordpress will not be found because it has no manifest.
	// cs:wordpress-simple will not be found because it is a bundle.
	c.Assert(resp.Results, gc.HasLen, len(storetesting.SearchEntities)-3)

	c.Assert(tw.Log(), jc.LogMatches, []string{"cannot retrieve metadata for cs:" + storetesting.SearchSeries[0] + "/wordpress-23: cannot open archive data for cs:" + storetesting.SearchSeries[0] + "/wordpress-23: .*"})
}

func (s *SearchSuite) TestSorting(c *gc.C) {
	tests := []struct {
		about   string
		query   string
		results []*router.ResolvedURL
	}{{
		about: "name ascending",
		query: "sort=name",
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
			false,
		),
	}, {
		about: "name descending",
		query: "sort=-name",
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
			false,
		),
	}, {
		about: "series ascending",
		query: "sort=series,name",
		results: storetesting.ResolvedURLs(
			storetesting.SortBySeries(
				[]storetesting.SearchEntity{
					storetesting.SearchEntities["cloud-controller-worker-v2"],
					storetesting.SearchEntities["multi-series"],
					storetesting.SearchEntities["mysql"],
					storetesting.SearchEntities["squid-forwardproxy"],
					storetesting.SearchEntities["varnish"],
					storetesting.SearchEntities["wordpress"],
					storetesting.SearchEntities["wordpress-simple"],
				},
				false,
			),
			false,
		),
	}, {
		about: "series descending",
		query: "sort=-series&sort=name",
		results: storetesting.ResolvedURLs(
			storetesting.SortBySeries(
				[]storetesting.SearchEntity{
					storetesting.SearchEntities["cloud-controller-worker-v2"],
					storetesting.SearchEntities["multi-series"],
					storetesting.SearchEntities["mysql"],
					storetesting.SearchEntities["squid-forwardproxy"],
					storetesting.SearchEntities["varnish"],
					storetesting.SearchEntities["wordpress"],
					storetesting.SearchEntities["wordpress-simple"],
				},
				true,
			),
			false,
		),
	}, {
		about: "owner ascending",
		query: "sort=owner,name",
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
			false,
		),
	}, {
		about: "owner descending",
		query: "sort=-owner&sort=name",
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
			false,
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
		URL:      storeURL("search"),
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
		false,
	)
	var sr params.SearchResponse
	err := json.Unmarshal(rec.Body.Bytes(), &sr)
	c.Assert(err, gc.Equals, nil)
	assertResultSet(c, sr, expected)
}

func (s *SearchSuite) TestSearchWithUserMacaroon(c *gc.C) {
	rec := httptesting.DoRequest(c, httptesting.DoRequestParams{
		Handler: s.srv,
		URL:     storeURL("search"),
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
		false,
	)
	var sr params.SearchResponse
	err := json.Unmarshal(rec.Body.Bytes(), &sr)
	c.Assert(err, gc.Equals, nil)
	assertResultSet(c, sr, expected)
}

func (s *SearchSuite) TestSearchDoesNotCreateExtraMacaroons(c *gc.C) {
	// Ensure that there's a macaroon already in the store
	// that can be reused.
	_, err := s.store.Bakery.NewMacaroon(nil)
	c.Assert(err, gc.Equals, nil)
	n, err := s.store.DB.Macaroons().Find(nil).Count()
	c.Assert(err, gc.Equals, nil)
	c.Assert(n, gc.Equals, 1)
	rec := httptesting.DoRequest(c, httptesting.DoRequestParams{
		Handler: s.srv,
		URL:     storeURL("search"),
		Do:      s.bakeryDoAsUser("noone"),
	})
	c.Assert(rec.Code, gc.Equals, http.StatusOK)
	n, err = s.store.DB.Macaroons().Find(nil).Count()
	c.Assert(err, gc.Equals, nil)
	c.Assert(n, gc.Equals, 1)
}

func (s *SearchSuite) TestSearchWithUserInGroups(c *gc.C) {
	s.idmServer.AddUser("bob", "test-user", "test-user2")
	rec := httptesting.DoRequest(c, httptesting.DoRequestParams{
		Handler: s.srv,
		URL:     storeURL("search"),
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
		false,
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
		URL:      storeURL("search"),
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
		false,
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
