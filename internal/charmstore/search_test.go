// Copyright 2014 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package charmstore

import (
	"encoding/json"
	"sort"
	"strings"
	"sync"

	jc "github.com/juju/testing/checkers"
	gc "gopkg.in/check.v1"
	"gopkg.in/juju/charm.v6"
	"gopkg.in/juju/charmrepo.v3/csclient/params"

	"gopkg.in/juju/charmstore.v5/internal/mongodoc"
	"gopkg.in/juju/charmstore.v5/internal/router"
	"gopkg.in/juju/charmstore.v5/internal/storetesting"
)

type StoreSearchSuite struct {
	storetesting.IsolatedMgoESSuite
	pool  *Pool
	store *Store
	index SearchIndex
}

var _ = gc.Suite(&StoreSearchSuite{})

func (s *StoreSearchSuite) SetUpTest(c *gc.C) {
	s.IsolatedMgoESSuite.SetUpTest(c)
	s.index = SearchIndex{s.ES, s.TestIndex}
	err := s.ES.RefreshIndex(".versions")
	c.Assert(err, gc.Equals, nil)
	pool, err := NewPool(s.Session.DB("foo"), &s.index, nil, ServerParams{})
	c.Assert(err, gc.Equals, nil)
	s.pool = pool
	s.store = pool.Store()
	s.addEntities(c)
	c.Assert(err, gc.Equals, nil)
}

func (s *StoreSearchSuite) TearDownTest(c *gc.C) {
	if s.store != nil {
		s.store.Close()
	}
	if s.pool != nil {
		s.pool.Close()
	}
	s.IsolatedMgoESSuite.TearDownTest(c)
}

func (s *StoreSearchSuite) entity(c *gc.C, rurl *router.ResolvedURL) *mongodoc.Entity {
	e, err := s.store.FindEntity(rurl, nil)
	c.Assert(err, gc.Equals, nil)
	return e
}

func (s *StoreSearchSuite) addEntities(c *gc.C) {
	for _, ent := range storetesting.SearchEntities {
		if ent.URL.Series == "bundle" {
			continue
		}
		addCharmForSearch(c, s.store, ent.ResolvedURL(), ent.Charm, ent.ACL, ent.Downloads)

	}
	for _, ent := range storetesting.SearchEntities {
		if ent.URL.Series == "bundle" {
			addBundleForSearch(c, s.store, ent.ResolvedURL(), ent.Bundle, ent.ACL, ent.Downloads)
		}
	}
	s.store.pool.statsCache.EvictAll()
	err := s.store.syncSearch()
	c.Assert(err, gc.Equals, nil)
}

func (s *StoreSearchSuite) TestSuccessfulExport(c *gc.C) {
	s.store.pool.statsCache.EvictAll()
	for _, ent := range storetesting.SearchEntities {
		entity, err := s.store.FindEntity(ent.ResolvedURL(), nil)
		c.Assert(err, gc.Equals, nil)
		var actual json.RawMessage
		err = s.store.ES.GetDocument(s.TestIndex, typeName, s.store.ES.getID(entity.URL), &actual)
		c.Assert(err, gc.Equals, nil)
		series := entity.SupportedSeries
		if ent.URL.Series == "bundle" {
			series = []string{"bundle"}
		}
		doc := SearchDoc{
			Entity:         entity,
			TotalDownloads: int64(ent.Downloads),
			ReadACLs:       ent.ACL,
			Series:         series,
			AllSeries:      true,
			SingleSeries:   ent.URL.Series != "",
		}
		c.Assert(string(actual), jc.JSONEquals, doc)
	}
}

func (s *StoreSearchSuite) TestNoExportDeprecated(c *gc.C) {
	charmArchive := storetesting.NewCharm(nil)
	url := router.MustNewResolvedURL("cs:~charmers/saucy/mysql-4", -1)
	addCharmForSearch(
		c,
		s.store,
		url,
		charmArchive,
		nil,
		0,
	)
	var entity *mongodoc.Entity
	err := s.store.DB.Entities().FindId("cs:~openstack-charmers/" + storetesting.SearchSeries[2] + "/mysql-7").One(&entity)
	c.Assert(err, gc.Equals, nil)
	present, err := s.store.ES.HasDocument(s.TestIndex, typeName, s.store.ES.getID(entity.URL))
	c.Assert(err, gc.Equals, nil)
	c.Assert(present, gc.Equals, true)

	err = s.store.DB.Entities().FindId("cs:~charmers/saucy/mysql-4").One(&entity)
	c.Assert(err, gc.Equals, nil)
	present, err = s.store.ES.HasDocument(s.TestIndex, typeName, s.store.ES.getID(entity.URL))
	c.Assert(err, gc.Equals, nil)
	c.Assert(present, gc.Equals, false)
}

func (s *StoreSearchSuite) TestExportOnlyLatest(c *gc.C) {
	charmArchive := storetesting.NewCharm(nil)
	url := router.MustNewResolvedURL("cs:~charmers/"+storetesting.SearchSeries[0]+"/wordpress-24", -1)
	addCharmForSearch(
		c,
		s.store,
		url,
		charmArchive,
		[]string{"charmers", params.Everyone},
		0,
	)
	var expected, old *mongodoc.Entity
	var actual json.RawMessage
	err := s.store.DB.Entities().FindId("cs:~charmers/" + storetesting.SearchSeries[0] + "/wordpress-23").One(&old)
	c.Assert(err, gc.Equals, nil)
	err = s.store.DB.Entities().FindId("cs:~charmers/" + storetesting.SearchSeries[0] + "/wordpress-24").One(&expected)
	c.Assert(err, gc.Equals, nil)
	err = s.store.ES.GetDocument(s.TestIndex, typeName, s.store.ES.getID(old.URL), &actual)
	c.Assert(err, gc.Equals, nil)
	doc := SearchDoc{
		Entity:       expected,
		ReadACLs:     []string{"charmers", params.Everyone},
		Series:       expected.SupportedSeries,
		SingleSeries: true,
		AllSeries:    true,
	}
	c.Assert(string(actual), jc.JSONEquals, doc)
}

func (s *StoreSearchSuite) TestExportMultiSeriesCharmsCreateExpandedVersions(c *gc.C) {
	charmArchive := storetesting.NewCharm(nil)
	url := router.MustNewResolvedURL("cs:~charmers/"+storetesting.SearchSeries[2]+"/juju-gui-24", -1)
	addCharmForSearch(
		c,
		s.store,
		url,
		charmArchive,
		[]string{"charmers"},
		0,
	)
	charmArchive = storetesting.NewCharm(storetesting.MetaWithSupportedSeries(nil, storetesting.SearchSeries[1], storetesting.SearchSeries[2]))
	url = router.MustNewResolvedURL("cs:~charmers/juju-gui-25", -1)
	addCharmForSearch(
		c,
		s.store,
		url,
		charmArchive,
		[]string{"charmers"},
		0,
	)
	var expected, old *mongodoc.Entity
	var actual json.RawMessage
	err := s.store.DB.Entities().FindId("cs:~charmers/" + storetesting.SearchSeries[2] + "/juju-gui-24").One(&old)
	c.Assert(err, gc.Equals, nil)
	err = s.store.DB.Entities().FindId("cs:~charmers/juju-gui-25").One(&expected)
	c.Assert(err, gc.Equals, nil)
	err = s.store.ES.GetDocument(s.TestIndex, typeName, s.store.ES.getID(expected.URL), &actual)
	c.Assert(err, gc.Equals, nil)
	doc := SearchDoc{
		Entity:       expected,
		ReadACLs:     []string{"charmers"},
		Series:       expected.SupportedSeries,
		SingleSeries: false,
		AllSeries:    true,
	}
	c.Assert(string(actual), jc.JSONEquals, doc)
	err = s.store.ES.GetDocument(s.TestIndex, typeName, s.store.ES.getID(old.URL), &actual)
	c.Assert(err, gc.Equals, nil)
	expected.URL.Series = old.URL.Series
	doc = SearchDoc{
		Entity:       expected,
		ReadACLs:     []string{"charmers"},
		Series:       []string{old.URL.Series},
		SingleSeries: true,
		AllSeries:    false,
	}
	c.Assert(string(actual), jc.JSONEquals, doc)
}

func (s *StoreSearchSuite) TestExportSearchDocument(c *gc.C) {
	var entity *mongodoc.Entity
	var actual json.RawMessage
	err := s.store.DB.Entities().FindId("cs:~charmers/" + storetesting.SearchSeries[0] + "/wordpress-23").One(&entity)
	c.Assert(err, gc.Equals, nil)
	doc := SearchDoc{Entity: entity, TotalDownloads: 4000}
	err = s.store.ES.update(&doc)
	c.Assert(err, gc.Equals, nil)
	err = s.store.ES.GetDocument(s.TestIndex, typeName, s.store.ES.getID(entity.URL), &actual)
	c.Assert(err, gc.Equals, nil)
	c.Assert(string(actual), jc.JSONEquals, doc)
}

var searchTests = []struct {
	about     string
	sp        SearchParams
	results   []storetesting.SearchEntity
	totalDiff int // len(results) + totalDiff = expected total
}{
	{
		about: "basic text search",
		sp: SearchParams{
			Text: "wordpress",
		},
		results: []storetesting.SearchEntity{
			storetesting.SearchEntities["wordpress"],
			storetesting.SearchEntities["wordpress-simple"],
		},
	}, {
		about: "blank text search",
		sp: SearchParams{
			Text: "",
		},
		results: []storetesting.SearchEntity{
			storetesting.SearchEntities["multi-series"],
			storetesting.SearchEntities["cloud-controller-worker-v2"],
			storetesting.SearchEntities["wordpress"],
			storetesting.SearchEntities["mysql"],
			storetesting.SearchEntities["varnish"],
			storetesting.SearchEntities["squid-forwardproxy"],
			storetesting.SearchEntities["wordpress-simple"],
		},
	}, {
		about: "autocomplete search",
		sp: SearchParams{
			Text:         "word",
			AutoComplete: true,
		},
		results: []storetesting.SearchEntity{
			storetesting.SearchEntities["wordpress"],
			storetesting.SearchEntities["wordpress-simple"],
		},
	}, {
		about: "autocomplete case insensitive",
		sp: SearchParams{
			Text:         "woRd",
			AutoComplete: true,
		},
		results: []storetesting.SearchEntity{
			storetesting.SearchEntities["wordpress"],
			storetesting.SearchEntities["wordpress-simple"],
		},
	}, {
		about: "autocomplete end of word",
		sp: SearchParams{
			Text:         "PRESS",
			AutoComplete: true,
		},
		results: []storetesting.SearchEntity{
			storetesting.SearchEntities["wordpress"],
			storetesting.SearchEntities["wordpress-simple"],
		},
	}, {
		about: "non-matching autocomplete search",
		sp: SearchParams{
			Text:         "worm",
			AutoComplete: true,
		},
		results: []storetesting.SearchEntity{},
	}, {
		about: "autocomplete with hyphen - match",
		sp: SearchParams{
			Text:         "squid-f",
			AutoComplete: true,
		},
		results: []storetesting.SearchEntity{
			storetesting.SearchEntities["squid-forwardproxy"],
		},
	}, {
		about: "autocomplete with hyphen - no match",
		sp: SearchParams{
			Text:         "squid-g",
			AutoComplete: true,
		},
		results: []storetesting.SearchEntity{},
	}, {
		about: "description filter search",
		sp: SearchParams{
			Text: "",
			Filters: map[string][]string{
				"description": {"blog"},
			},
		},
		results: []storetesting.SearchEntity{
			storetesting.SearchEntities["wordpress"],
		},
	}, {
		about: "name filter search",
		sp: SearchParams{
			Text: "",
			Filters: map[string][]string{
				"name": {"wordpress"},
			},
		},
		results: []storetesting.SearchEntity{
			storetesting.SearchEntities["wordpress"],
		},
	}, {
		about: "owner filter search",
		sp: SearchParams{
			Text: "",
			Filters: map[string][]string{
				"owner": {"foo"},
			},
		},
		results: []storetesting.SearchEntity{
			storetesting.SearchEntities["varnish"],
		},
	}, {
		about: "provides filter search",
		sp: SearchParams{
			Text: "",
			Filters: map[string][]string{
				"provides": {"mysql"},
			},
		},
		results: []storetesting.SearchEntity{
			storetesting.SearchEntities["mysql"],
		},
	}, {
		about: "requires filter search",
		sp: SearchParams{
			Text: "",
			Filters: map[string][]string{
				"requires": {"mysql"},
			},
		},
		results: []storetesting.SearchEntity{
			storetesting.SearchEntities["multi-series"],
			storetesting.SearchEntities["wordpress"],
		},
	}, {
		about: "series filter search",
		sp: SearchParams{
			Text: "",
			Filters: map[string][]string{
				"series": {storetesting.SearchSeries[2]},
			},
		},
		results: []storetesting.SearchEntity{
			storetesting.SearchEntities["multi-series"],
			storetesting.SearchEntities["squid-forwardproxy"],
			storetesting.SearchEntities["varnish"],
			storetesting.SearchEntities["mysql"],
		},
	}, {
		about: "summary filter search",
		sp: SearchParams{
			Text: "",
			Filters: map[string][]string{
				"summary": {"Database engine"},
			},
		},
		results: []storetesting.SearchEntity{
			storetesting.SearchEntities["mysql"],
			storetesting.SearchEntities["varnish"],
		},
	}, {
		about: "tags filter search",
		sp: SearchParams{
			Text: "",
			Filters: map[string][]string{
				"tags": {"wordpress"},
			},
		},
		results: []storetesting.SearchEntity{
			storetesting.SearchEntities["wordpress"],
			storetesting.SearchEntities["wordpress-simple"],
		},
	}, {
		about: "bundle type filter search",
		sp: SearchParams{
			Text: "",
			Filters: map[string][]string{
				"type": {"bundle"},
			},
		},
		results: []storetesting.SearchEntity{
			storetesting.SearchEntities["wordpress-simple"],
		},
	}, {
		about: "charm type filter search",
		sp: SearchParams{
			Text: "",
			Filters: map[string][]string{
				"type": {"charm"},
			},
		},
		results: []storetesting.SearchEntity{
			storetesting.SearchEntities["multi-series"],
			storetesting.SearchEntities["cloud-controller-worker-v2"],
			storetesting.SearchEntities["wordpress"],
			storetesting.SearchEntities["mysql"],
			storetesting.SearchEntities["varnish"],
			storetesting.SearchEntities["squid-forwardproxy"],
		},
	}, {
		about: "charm & bundle type filter search",
		sp: SearchParams{
			Text: "",
			Filters: map[string][]string{
				"type": {"charm", "bundle"},
			},
		},
		results: []storetesting.SearchEntity{
			storetesting.SearchEntities["multi-series"],
			storetesting.SearchEntities["cloud-controller-worker-v2"],
			storetesting.SearchEntities["wordpress"],
			storetesting.SearchEntities["mysql"],
			storetesting.SearchEntities["varnish"],
			storetesting.SearchEntities["squid-forwardproxy"],
			storetesting.SearchEntities["wordpress-simple"],
		},
	}, {
		about: "invalid filter search",
		sp: SearchParams{
			Text: "",
			Filters: map[string][]string{
				"no such filter": {"foo"},
			},
		},
		results: []storetesting.SearchEntity{
			storetesting.SearchEntities["multi-series"],
			storetesting.SearchEntities["cloud-controller-worker-v2"],
			storetesting.SearchEntities["wordpress"],
			storetesting.SearchEntities["mysql"],
			storetesting.SearchEntities["varnish"],
			storetesting.SearchEntities["squid-forwardproxy"],
			storetesting.SearchEntities["wordpress-simple"],
		},
	}, {
		about: "valid & invalid filter search",
		sp: SearchParams{
			Text: "",
			Filters: map[string][]string{
				"no such filter": {"foo"},
				"type":           {"charm"},
			},
		},
		results: []storetesting.SearchEntity{
			storetesting.SearchEntities["multi-series"],
			storetesting.SearchEntities["cloud-controller-worker-v2"],
			storetesting.SearchEntities["wordpress"],
			storetesting.SearchEntities["mysql"],
			storetesting.SearchEntities["varnish"],
			storetesting.SearchEntities["squid-forwardproxy"],
		},
	}, {
		about: "paginated search",
		sp: SearchParams{
			Filters: map[string][]string{
				"name": {"mysql"},
			},
			Skip: 1,
		},
		totalDiff: +1,
	}, {
		about: "additional groups",
		sp: SearchParams{
			Groups: []string{"charmers"},
		},
		results: []storetesting.SearchEntity{
			storetesting.SearchEntities["multi-series"],
			storetesting.SearchEntities["riak"],
			storetesting.SearchEntities["cloud-controller-worker-v2"],
			storetesting.SearchEntities["wordpress"],
			storetesting.SearchEntities["mysql"],
			storetesting.SearchEntities["varnish"],
			storetesting.SearchEntities["squid-forwardproxy"],
			storetesting.SearchEntities["wordpress-simple"],
		},
	}, {
		about: "admin search",
		sp: SearchParams{
			Admin: true,
		},
		results: []storetesting.SearchEntity{
			storetesting.SearchEntities["multi-series"],
			storetesting.SearchEntities["riak"],
			storetesting.SearchEntities["cloud-controller-worker-v2"],
			storetesting.SearchEntities["wordpress"],
			storetesting.SearchEntities["mysql"],
			storetesting.SearchEntities["varnish"],
			storetesting.SearchEntities["squid-forwardproxy"],
			storetesting.SearchEntities["wordpress-simple"],
		},
	}, {
		about: "charm tags filter search",
		sp: SearchParams{
			Text: "",
			Filters: map[string][]string{
				"tags": {"wordpressTAG"},
			},
		},
		results: []storetesting.SearchEntity{
			storetesting.SearchEntities["wordpress"],
		},
	}, {
		about: "blank owner filter search",
		sp: SearchParams{
			Text: "",
			Filters: map[string][]string{
				"owner": {""},
			},
		},
		results: []storetesting.SearchEntity{
			storetesting.SearchEntities["multi-series"],
			storetesting.SearchEntities["wordpress"],
			storetesting.SearchEntities["mysql"],
			storetesting.SearchEntities["squid-forwardproxy"],
			storetesting.SearchEntities["wordpress-simple"],
		},
	}, {
		about: "promulgated search",
		sp: SearchParams{
			Text: "",
			Filters: map[string][]string{
				"promulgated": {"1"},
			},
		},
		results: []storetesting.SearchEntity{
			storetesting.SearchEntities["multi-series"],
			storetesting.SearchEntities["wordpress"],
			storetesting.SearchEntities["mysql"],
			storetesting.SearchEntities["squid-forwardproxy"],
			storetesting.SearchEntities["wordpress-simple"],
		},
	}, {
		about: "not promulgated search",
		sp: SearchParams{
			Text: "",
			Filters: map[string][]string{
				"promulgated": {"0"},
			},
		},
		results: []storetesting.SearchEntity{
			storetesting.SearchEntities["cloud-controller-worker-v2"],
			storetesting.SearchEntities["varnish"],
		},
	}, {
		about: "owner and promulgated filter search",
		sp: SearchParams{
			Text: "",
			Filters: map[string][]string{
				"promulgated": {"1"},
				"owner":       {"openstack-charmers"},
			},
		},
		results: []storetesting.SearchEntity{
			storetesting.SearchEntities["mysql"],
		},
	}, {
		about: "name search",
		sp: SearchParams{
			Text: "wordpress",
		},
		results: []storetesting.SearchEntity{
			storetesting.SearchEntities["wordpress"],
			storetesting.SearchEntities["wordpress-simple"],
		},
	}, {
		about: "case insensitive search",
		sp: SearchParams{
			Text: "WORDPRESS",
		},
		results: []storetesting.SearchEntity{
			storetesting.SearchEntities["wordpress"],
			storetesting.SearchEntities["wordpress-simple"],
		},
	}, {
		about: "case insensitive search on tags",
		sp: SearchParams{
			Text: "WORDPRESSTAG",
		},
		results: []storetesting.SearchEntity{
			storetesting.SearchEntities["wordpress"],
		},
	}, {
		about: "case insensitive search on categories",
		sp: SearchParams{
			Text: "WORDPRESSCAT",
		},
		results: []storetesting.SearchEntity{
			storetesting.SearchEntities["wordpress"],
		},
	}, {
		about: "autocomplete with spaces",
		sp: SearchParams{
			Text:         "wordpress simple",
			AutoComplete: true,
		},
		results: []storetesting.SearchEntity{
			storetesting.SearchEntities["wordpress-simple"],
		},
	}, {
		about: "autocomplete with spaces, reversed",
		sp: SearchParams{
			Text:         "simple wordpress",
			AutoComplete: true,
		},
		results: []storetesting.SearchEntity{
			storetesting.SearchEntities["wordpress-simple"],
		},
	},
}

func (s *StoreSearchSuite) TestSearches(c *gc.C) {
	s.store.ES.Database.RefreshIndex(s.TestIndex)
	for i, test := range searchTests {
		c.Logf("test %d: %s", i, test.about)
		total, res := search(c, s.store, test.sp)
		sort.Sort(resolvedURLsByString(res))
		expected := make(Entities, len(test.results))
		for i, r := range test.results {
			expected[i] = s.entity(c, r.ResolvedURL())
		}
		sort.Sort(resolvedURLsByString(expected))
		c.Check(Entities(res), jc.DeepEquals, expected)
		c.Check(total, gc.Equals, len(test.results)+test.totalDiff)
	}
}

type resolvedURLsByString Entities

func (r resolvedURLsByString) Less(i, j int) bool {
	return r[i].URL.String() < r[j].URL.String()
}

func (r resolvedURLsByString) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}

func (r resolvedURLsByString) Len() int {
	return len(r)
}

func (s *StoreSearchSuite) TestPaginatedSearch(c *gc.C) {
	err := s.store.ES.Database.RefreshIndex(s.TestIndex)
	c.Assert(err, gc.Equals, nil)
	sp := SearchParams{
		Text: "wordpress",
		Skip: 1,
	}
	total, res := search(c, s.store, sp)
	c.Assert(res, gc.HasLen, 1)
	c.Assert(total, gc.Equals, 2)
}

func (s *StoreSearchSuite) TestLimitTestSearch(c *gc.C) {
	err := s.store.ES.Database.RefreshIndex(s.TestIndex)
	c.Assert(err, gc.Equals, nil)
	sp := SearchParams{
		Text:  "wordpress",
		Limit: 1,
	}
	_, res := search(c, s.store, sp)
	c.Assert(res, gc.HasLen, 1)
}

func (s *StoreSearchSuite) TestPromulgatedRank(c *gc.C) {
	ent := storetesting.SearchEntity{
		URL:                 charm.MustParseURL("cs:~charmers/" + storetesting.SearchSeries[2] + "/varnish-1"),
		PromulgatedRevision: 1,
		Charm:               storetesting.NewCharm(nil),
		ACL:                 []string{"charmers", params.Everyone},
	}
	addCharmForSearch(c, s.store, ent.ResolvedURL(), ent.Charm, ent.ACL, ent.Downloads)
	s.store.ES.Database.RefreshIndex(s.TestIndex)
	sp := SearchParams{
		Filters: map[string][]string{
			"name": {"varnish"},
		},
	}
	_, res := search(c, s.store, sp)
	c.Assert(Entities(res), jc.DeepEquals, Entities{
		s.entity(c, ent.ResolvedURL()),
		s.entity(c, storetesting.SearchEntities["varnish"].ResolvedURL()),
	})
}

func (s *StoreSearchSuite) TestSorting(c *gc.C) {
	s.store.ES.Database.RefreshIndex(s.TestIndex)
	tests := []struct {
		about     string
		sortQuery string
		results   []storetesting.SearchEntity
	}{{
		about:     "name ascending",
		sortQuery: "name",
		results: []storetesting.SearchEntity{
			storetesting.SearchEntities["cloud-controller-worker-v2"],
			storetesting.SearchEntities["multi-series"],
			storetesting.SearchEntities["mysql"],
			storetesting.SearchEntities["squid-forwardproxy"],
			storetesting.SearchEntities["varnish"],
			storetesting.SearchEntities["wordpress"],
			storetesting.SearchEntities["wordpress-simple"],
		},
	}, {
		about:     "name descending",
		sortQuery: "-name",
		results: []storetesting.SearchEntity{
			storetesting.SearchEntities["wordpress-simple"],
			storetesting.SearchEntities["wordpress"],
			storetesting.SearchEntities["varnish"],
			storetesting.SearchEntities["squid-forwardproxy"],
			storetesting.SearchEntities["mysql"],
			storetesting.SearchEntities["multi-series"],
			storetesting.SearchEntities["cloud-controller-worker-v2"],
		},
	}, {
		about:     "series ascending",
		sortQuery: "series,name",
		results: storetesting.SortBySeries([]storetesting.SearchEntity{
			storetesting.SearchEntities["cloud-controller-worker-v2"],
			storetesting.SearchEntities["multi-series"],
			storetesting.SearchEntities["mysql"],
			storetesting.SearchEntities["squid-forwardproxy"],
			storetesting.SearchEntities["varnish"],
			storetesting.SearchEntities["wordpress"],
			storetesting.SearchEntities["wordpress-simple"],
		}, false),
	}, {
		about:     "series descending",
		sortQuery: "-series,name",
		results: storetesting.SortBySeries([]storetesting.SearchEntity{
			storetesting.SearchEntities["cloud-controller-worker-v2"],
			storetesting.SearchEntities["multi-series"],
			storetesting.SearchEntities["mysql"],
			storetesting.SearchEntities["squid-forwardproxy"],
			storetesting.SearchEntities["varnish"],
			storetesting.SearchEntities["wordpress"],
			storetesting.SearchEntities["wordpress-simple"],
		}, true),
	}, {
		about:     "owner ascending",
		sortQuery: "owner,name",
		results: []storetesting.SearchEntity{
			storetesting.SearchEntities["cloud-controller-worker-v2"],
			storetesting.SearchEntities["multi-series"],
			storetesting.SearchEntities["squid-forwardproxy"],
			storetesting.SearchEntities["wordpress"],
			storetesting.SearchEntities["wordpress-simple"],
			storetesting.SearchEntities["varnish"],
			storetesting.SearchEntities["mysql"],
		},
	}, {
		about:     "owner descending",
		sortQuery: "-owner,name",
		results: []storetesting.SearchEntity{
			storetesting.SearchEntities["mysql"],
			storetesting.SearchEntities["varnish"],
			storetesting.SearchEntities["multi-series"],
			storetesting.SearchEntities["squid-forwardproxy"],
			storetesting.SearchEntities["wordpress"],
			storetesting.SearchEntities["wordpress-simple"],
			storetesting.SearchEntities["cloud-controller-worker-v2"],
		},
	}, {
		about:     "downloads ascending",
		sortQuery: "downloads,name",
		results: []storetesting.SearchEntity{
			storetesting.SearchEntities["multi-series"],
			storetesting.SearchEntities["wordpress"],
			storetesting.SearchEntities["wordpress-simple"],
			storetesting.SearchEntities["squid-forwardproxy"],
			storetesting.SearchEntities["mysql"],
			storetesting.SearchEntities["cloud-controller-worker-v2"],
			storetesting.SearchEntities["varnish"],
		},
	}, {
		about:     "downloads descending",
		sortQuery: "-downloads,name",
		results: []storetesting.SearchEntity{
			storetesting.SearchEntities["varnish"],
			storetesting.SearchEntities["cloud-controller-worker-v2"],
			storetesting.SearchEntities["mysql"],
			storetesting.SearchEntities["squid-forwardproxy"],
			storetesting.SearchEntities["wordpress-simple"],
			storetesting.SearchEntities["multi-series"],
			storetesting.SearchEntities["wordpress"],
		},
	}}
	for i, test := range tests {
		c.Logf("test %d. %s", i, test.about)
		var sp SearchParams
		err := sp.ParseSortFields(test.sortQuery)
		c.Assert(err, gc.Equals, nil)
		total, res := search(c, s.store, sp)
		expected := make([]*mongodoc.Entity, len(test.results))
		for i, r := range test.results {
			expected[i] = s.entity(c, r.ResolvedURL())
		}
		c.Assert(Entities(res), jc.DeepEquals, Entities(expected))
		c.Assert(total, gc.Equals, len(test.results))
	}
}

func (s *StoreSearchSuite) TestBoosting(c *gc.C) {
	err := s.store.ES.Database.RefreshIndex(s.TestIndex)
	c.Assert(err, gc.Equals, nil)
	var sp SearchParams
	_, res := search(c, s.store, sp)
	c.Assert(Entities(res), jc.DeepEquals, Entities{
		s.entity(c, storetesting.SearchEntities["multi-series"].ResolvedURL()),
		s.entity(c, storetesting.SearchEntities["wordpress-simple"].ResolvedURL()),
		s.entity(c, storetesting.SearchEntities["wordpress"].ResolvedURL()),
		s.entity(c, storetesting.SearchEntities["mysql"].ResolvedURL()),
		s.entity(c, storetesting.SearchEntities["squid-forwardproxy"].ResolvedURL()),
		s.entity(c, storetesting.SearchEntities["cloud-controller-worker-v2"].ResolvedURL()),
		s.entity(c, storetesting.SearchEntities["varnish"].ResolvedURL()),
	})
}

func (s *StoreSearchSuite) TestEnsureIndex(c *gc.C) {
	s.store.ES.Index = s.TestIndex + "-ensure-index"
	defer s.ES.DeleteDocument(".versions", "version", s.store.ES.Index)
	indexes, err := s.ES.ListIndexesForAlias(s.store.ES.Index)
	c.Assert(err, gc.Equals, nil)
	c.Assert(indexes, gc.HasLen, 0)
	err = s.store.ES.ensureIndexes(false)
	c.Assert(err, gc.Equals, nil)
	indexes, err = s.ES.ListIndexesForAlias(s.store.ES.Index)
	c.Assert(err, gc.Equals, nil)
	c.Assert(indexes, gc.HasLen, 1)
	index := indexes[0]
	err = s.store.ES.ensureIndexes(false)
	c.Assert(err, gc.Equals, nil)
	indexes, err = s.ES.ListIndexesForAlias(s.store.ES.Index)
	c.Assert(err, gc.Equals, nil)
	c.Assert(indexes, gc.HasLen, 1)
	c.Assert(indexes[0], gc.Equals, index)
}

func (s *StoreSearchSuite) TestEnsureConcurrent(c *gc.C) {
	s.store.ES.Index = s.TestIndex + "-ensure-index-conc"
	defer s.ES.DeleteDocument(".versions", "version", s.store.ES.Index)
	indexes, err := s.ES.ListIndexesForAlias(s.store.ES.Index)
	c.Assert(err, gc.Equals, nil)
	c.Assert(indexes, gc.HasLen, 0)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		err := s.store.ES.ensureIndexes(false)
		c.Check(err, gc.Equals, nil)
		wg.Done()
	}()
	err = s.store.ES.ensureIndexes(false)
	c.Assert(err, gc.Equals, nil)
	wg.Wait()
	indexes, err = s.ES.ListIndexesForAlias(s.store.ES.Index)
	c.Assert(err, gc.Equals, nil)
	c.Assert(indexes, gc.HasLen, 1)
}

func (s *StoreSearchSuite) TestEnsureIndexForce(c *gc.C) {
	s.store.ES.Index = s.TestIndex + "-ensure-index-force"
	defer s.ES.DeleteDocument(".versions", "version", s.store.ES.Index)
	indexes, err := s.ES.ListIndexesForAlias(s.store.ES.Index)
	c.Assert(err, gc.Equals, nil)
	c.Assert(indexes, gc.HasLen, 0)
	err = s.store.ES.ensureIndexes(false)
	c.Assert(err, gc.Equals, nil)
	indexes, err = s.ES.ListIndexesForAlias(s.store.ES.Index)
	c.Assert(err, gc.Equals, nil)
	c.Assert(indexes, gc.HasLen, 1)
	index := indexes[0]
	err = s.store.ES.ensureIndexes(true)
	c.Assert(err, gc.Equals, nil)
	indexes, err = s.ES.ListIndexesForAlias(s.store.ES.Index)
	c.Assert(err, gc.Equals, nil)
	c.Assert(indexes, gc.HasLen, 1)
	c.Assert(indexes[0], gc.Not(gc.Equals), index)
}

func (s *StoreSearchSuite) TestGetCurrentVersionNoVersion(c *gc.C) {
	s.store.ES.Index = s.TestIndex + "-current-version"
	defer s.ES.DeleteDocument(".versions", "version", s.store.ES.Index)
	v, dv, err := s.store.ES.getCurrentVersion()
	c.Assert(err, gc.Equals, nil)
	c.Assert(v, gc.Equals, version{})
	c.Assert(dv, gc.Equals, int64(0))
}

func (s *StoreSearchSuite) TestGetCurrentVersionWithVersion(c *gc.C) {
	s.store.ES.Index = s.TestIndex + "-current-version"
	defer s.ES.DeleteDocument(".versions", "version", s.store.ES.Index)
	index, err := s.store.ES.newIndex()
	c.Assert(err, gc.Equals, nil)
	updated, err := s.store.ES.updateVersion(version{1, index}, 0)
	c.Assert(err, gc.Equals, nil)
	c.Assert(updated, gc.Equals, true)
	v, dv, err := s.store.ES.getCurrentVersion()
	c.Assert(err, gc.Equals, nil)
	c.Assert(v, gc.Equals, version{1, index})
	c.Assert(dv, gc.Equals, int64(1))
}

func (s *StoreSearchSuite) TestUpdateVersionNew(c *gc.C) {
	s.store.ES.Index = s.TestIndex + "-update-version"
	defer s.ES.DeleteDocument(".versions", "version", s.store.ES.Index)
	index, err := s.store.ES.newIndex()
	c.Assert(err, gc.Equals, nil)
	updated, err := s.store.ES.updateVersion(version{1, index}, 0)
	c.Assert(err, gc.Equals, nil)
	c.Assert(updated, gc.Equals, true)
}

func (s *StoreSearchSuite) TestUpdateVersionUpdate(c *gc.C) {
	s.store.ES.Index = s.TestIndex + "-update-version"
	defer s.ES.DeleteDocument(".versions", "version", s.store.ES.Index)
	index, err := s.store.ES.newIndex()
	c.Assert(err, gc.Equals, nil)
	updated, err := s.store.ES.updateVersion(version{1, index}, 0)
	c.Assert(err, gc.Equals, nil)
	c.Assert(updated, gc.Equals, true)
	index, err = s.store.ES.newIndex()
	c.Assert(err, gc.Equals, nil)
	updated, err = s.store.ES.updateVersion(version{2, index}, 1)
	c.Assert(err, gc.Equals, nil)
	c.Assert(updated, gc.Equals, true)
}

func (s *StoreSearchSuite) TestUpdateCreateConflict(c *gc.C) {
	s.store.ES.Index = s.TestIndex + "-update-version"
	defer s.ES.DeleteDocument(".versions", "version", s.store.ES.Index)
	index, err := s.store.ES.newIndex()
	c.Assert(err, gc.Equals, nil)
	updated, err := s.store.ES.updateVersion(version{1, index}, 0)
	c.Assert(err, gc.Equals, nil)
	c.Assert(updated, gc.Equals, true)
	index, err = s.store.ES.newIndex()
	c.Assert(err, gc.Equals, nil)
	updated, err = s.store.ES.updateVersion(version{1, index}, 0)
	c.Assert(err, gc.Equals, nil)
	c.Assert(updated, gc.Equals, false)
}

func (s *StoreSearchSuite) TestUpdateConflict(c *gc.C) {
	s.store.ES.Index = s.TestIndex + "-update-version"
	defer s.ES.DeleteDocument(".versions", "version", s.store.ES.Index)
	index, err := s.store.ES.newIndex()
	c.Assert(err, gc.Equals, nil)
	updated, err := s.store.ES.updateVersion(version{1, index}, 0)
	c.Assert(err, gc.Equals, nil)
	c.Assert(updated, gc.Equals, true)
	index, err = s.store.ES.newIndex()
	c.Assert(err, gc.Equals, nil)
	updated, err = s.store.ES.updateVersion(version{1, index}, 3)
	c.Assert(err, gc.Equals, nil)
	c.Assert(updated, gc.Equals, false)
}

func (s *StoreSearchSuite) TestMultiSeriesCharmFiltersSeriesCorrectly(c *gc.C) {
	charmArchive := storetesting.NewCharm(storetesting.MetaWithSupportedSeries(nil, storetesting.SearchSeries[1], storetesting.SearchSeries[2]))
	url := router.MustNewResolvedURL("cs:~charmers/juju-gui-25", -1)
	addCharmForSearch(
		c,
		s.store,
		url,
		charmArchive,
		[]string{url.URL.User, params.Everyone},
		0,
	)
	s.store.ES.Database.RefreshIndex(s.TestIndex)
	filterTests := []struct {
		series   string
		notFound bool
	}{{
		series: storetesting.SearchSeries[2],
	}, {
		series: storetesting.SearchSeries[1],
	}, {
		series:   "nosuch",
		notFound: true,
	}}
	for i, test := range filterTests {
		c.Logf("%d. %s", i, test.series)
		_, res := search(c, s.store, SearchParams{
			Filters: map[string][]string{
				"name":   {"juju-gui"},
				"series": {test.series},
			},
		})
		if test.notFound {
			c.Assert(res, gc.HasLen, 0)
			continue
		}
		c.Assert(res, gc.HasLen, 1)
		c.Assert(res[0].URL.String(), gc.Equals, url.String())
	}
}

func (s *StoreSearchSuite) TestOnlyIndexStableCharms(c *gc.C) {
	ch := storetesting.NewCharm(&charm.Meta{
		Name: "test",
	})
	id := router.MustNewResolvedURL("~test/"+storetesting.SearchSeries[2]+"/test-0", -1)
	err := s.store.AddCharmWithArchive(id, ch)
	c.Assert(err, gc.Equals, nil)
	err = s.store.SetPerms(&id.URL, "read", "test", params.Everyone)
	c.Assert(err, gc.Equals, nil)
	err = s.store.SetPerms(&id.URL, "edge.read", "test", params.Everyone)
	c.Assert(err, gc.Equals, nil)
	err = s.store.SetPerms(&id.URL, "stable.read", "test", params.Everyone)
	c.Assert(err, gc.Equals, nil)

	var actual json.RawMessage

	err = s.store.UpdateSearch(id)
	c.Assert(err, gc.Equals, nil)
	err = s.store.ES.GetDocument(s.TestIndex, typeName, s.store.ES.getID(&id.URL), &actual)
	c.Assert(err, gc.ErrorMatches, "not found")

	err = s.store.Publish(id, nil, params.EdgeChannel)
	c.Assert(err, gc.Equals, nil)
	err = s.store.UpdateSearch(id)
	c.Assert(err, gc.Equals, nil)
	err = s.store.ES.GetDocument(s.TestIndex, typeName, s.store.ES.getID(&id.URL), &actual)
	c.Assert(err, gc.ErrorMatches, "not found")

	err = s.store.Publish(id, nil, params.StableChannel)
	c.Assert(err, gc.Equals, nil)
	err = s.store.UpdateSearch(id)
	c.Assert(err, gc.Equals, nil)
	err = s.store.ES.GetDocument(s.TestIndex, typeName, s.store.ES.getID(&id.URL), &actual)
	c.Assert(err, gc.Equals, nil)

	entity, err := s.store.FindEntity(id, nil)
	c.Assert(err, gc.Equals, nil)
	doc := SearchDoc{
		Entity:       entity,
		ReadACLs:     []string{"test", params.Everyone},
		Series:       []string{storetesting.SearchSeries[2]},
		AllSeries:    true,
		SingleSeries: true,
	}
	c.Assert(string(actual), jc.JSONEquals, doc)
}

// addCharmForSearch adds a charm to the specified store such that it
// will be indexed in search. In order that it is indexed it is
// automatically published on the stable channel.
func addCharmForSearch(c *gc.C, s *Store, id *router.ResolvedURL, ch charm.Charm, acl []string, downloads int) {
	err := s.AddCharmWithArchive(id, ch)
	c.Assert(err, gc.Equals, nil)
	for i := 0; i < downloads; i++ {
		err := s.IncrementDownloadCounts(id)
		c.Assert(err, gc.Equals, nil)
	}
	err = s.SetPerms(&id.URL, "stable.read", acl...)
	c.Assert(err, gc.Equals, nil)
	err = s.Publish(id, nil, params.StableChannel)
	c.Assert(err, gc.Equals, nil)
}

// addBundleForSearch adds a bundle to the specified store such that it
// will be indexed in search. In order that it is indexed it is
// automatically published on the stable channel.
func addBundleForSearch(c *gc.C, s *Store, id *router.ResolvedURL, b charm.Bundle, acl []string, downloads int) {
	err := s.AddBundleWithArchive(id, b)
	c.Assert(err, gc.Equals, nil)
	for i := 0; i < downloads; i++ {
		err := s.IncrementDownloadCounts(id)
		c.Assert(err, gc.Equals, nil)
	}
	err = s.SetPerms(&id.URL, "stable.read", acl...)
	c.Assert(err, gc.Equals, nil)
	err = s.Publish(id, nil, params.StableChannel)
	c.Assert(err, gc.Equals, nil)
}

type Entities []*mongodoc.Entity

func (es Entities) GoString() string {
	return es.String()
}

func (es Entities) String() string {
	urls := make([]string, len(es))
	for i, e := range es {
		urls[i] = e.URL.String()
	}
	return "[" + strings.Join(urls, ", ") + "]"
}

func search(c *gc.C, store *Store, params SearchParams) (int, []*mongodoc.Entity) {
	q := store.SearchQuery(params)
	var entities []*mongodoc.Entity
	fields := make(map[string]int, len(searchFields))
	for k := range searchFields {
		fields[k] = 1
	}
	it := q.Iter(fields)
	var e mongodoc.Entity
	for it.Next(&e) {
		e := e
		entities = append(entities, &e)
	}
	c.Assert(it.Err(), gc.Equals, nil)
	return q.Total(), entities
}
