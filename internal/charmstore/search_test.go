// Copyright 2014 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package charmstore // import "gopkg.in/juju/charmstore.v5-unstable/internal/charmstore"

import (
	"encoding/json"
	"sort"
	"strings"
	"sync"

	jc "github.com/juju/testing/checkers"
	gc "gopkg.in/check.v1"
	"gopkg.in/juju/charm.v6-unstable"
	"gopkg.in/juju/charmrepo.v2-unstable/csclient/params"

	"gopkg.in/juju/charmstore.v5-unstable/internal/mongodoc"
	"gopkg.in/juju/charmstore.v5-unstable/internal/router"
	"gopkg.in/juju/charmstore.v5-unstable/internal/storetesting"
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

	// Temporarily set LegacyDownloadCountsEnabled to false, so that the real
	// code path can be reached by tests in this suite.
	// TODO (frankban): remove this block when removing the legacy counts
	// logic.
	original := LegacyDownloadCountsEnabled
	LegacyDownloadCountsEnabled = false
	s.AddCleanup(func(*gc.C) {
		LegacyDownloadCountsEnabled = original
	})

	s.index = SearchIndex{s.ES, s.TestIndex}
	s.ES.RefreshIndex(".versions")
	pool, err := NewPool(s.Session.DB("foo"), &s.index, nil, ServerParams{})
	c.Assert(err, gc.IsNil)
	s.pool = pool
	s.store = pool.Store()
	s.addCharmsToStore(c)
	c.Assert(err, gc.IsNil)
}

func (s *StoreSearchSuite) TearDownTest(c *gc.C) {
	s.store.Close()
	s.pool.Close()
	s.IsolatedMgoESSuite.TearDownTest(c)
}

func newEntity(id string, promulgatedRevision int, supportedSeries ...string) *mongodoc.Entity {
	url := charm.MustParseURL(id)
	var purl *charm.URL
	if promulgatedRevision > -1 {
		purl = new(charm.URL)
		*purl = *url
		purl.User = ""
		purl.Revision = promulgatedRevision
	}
	if url.Series == "bundle" {
		supportedSeries = nil
	} else if url.Series != "" {
		supportedSeries = []string{url.Series}
	}
	return &mongodoc.Entity{
		URL:                 url,
		SupportedSeries:     supportedSeries,
		PromulgatedURL:      purl,
		PromulgatedRevision: promulgatedRevision,
	}
}

var exportTestCharms = map[string]*mongodoc.Entity{
	"wordpress": newEntity("cs:~charmers/precise/wordpress-23", 23),
	"mysql":     newEntity("cs:~openstack-charmers/trusty/mysql-7", 7),
	"varnish":   newEntity("cs:~foo/trusty/varnish-1", -1),
	"riak":      newEntity("cs:~charmers/trusty/riak-67", 67),
}

var exportTestBundles = map[string]*mongodoc.Entity{
	"wordpress-simple": newEntity("cs:~charmers/bundle/wordpress-simple-4", 4),
}

var charmDownloadCounts = map[string]int{
	"wordpress":        0,
	"wordpress-simple": 1,
	"mysql":            3,
	"varnish":          5,
}

func (s *StoreSearchSuite) TestSuccessfulExport(c *gc.C) {
	s.store.pool.statsCache.EvictAll()
	for name, ent := range exportTestCharms {
		entity, err := s.store.FindEntity(EntityResolvedURL(ent), nil)
		c.Assert(err, gc.IsNil)
		var actual json.RawMessage
		err = s.store.ES.GetDocument(s.TestIndex, typeName, s.store.ES.getID(entity.URL), &actual)
		c.Assert(err, gc.IsNil)
		readACLs := []string{ent.URL.User, params.Everyone}
		if ent.URL.Name == "riak" {
			readACLs = []string{ent.URL.User}
		}
		doc := SearchDoc{
			Entity:         entity,
			TotalDownloads: int64(charmDownloadCounts[name]),
			ReadACLs:       readACLs,
			Series:         entity.SupportedSeries,
			AllSeries:      true,
			SingleSeries:   true,
		}
		c.Assert(string(actual), jc.JSONEquals, doc)
	}
}

func (s *StoreSearchSuite) TestNoExportDeprecated(c *gc.C) {
	charmArchive := storetesting.Charms.CharmDir("mysql")
	url := router.MustNewResolvedURL("cs:~charmers/saucy/mysql-4", -1)
	err := s.store.AddCharmWithArchive(url, charmArchive)
	c.Assert(err, gc.IsNil)

	var entity *mongodoc.Entity
	err = s.store.DB.Entities().FindId("cs:~openstack-charmers/trusty/mysql-7").One(&entity)
	c.Assert(err, gc.IsNil)
	present, err := s.store.ES.HasDocument(s.TestIndex, typeName, s.store.ES.getID(entity.URL))
	c.Assert(err, gc.IsNil)
	c.Assert(present, gc.Equals, true)

	err = s.store.DB.Entities().FindId("cs:~charmers/saucy/mysql-4").One(&entity)
	c.Assert(err, gc.IsNil)
	present, err = s.store.ES.HasDocument(s.TestIndex, typeName, s.store.ES.getID(entity.URL))
	c.Assert(err, gc.IsNil)
	c.Assert(present, gc.Equals, false)
}

func (s *StoreSearchSuite) TestNoExportUnpublished(c *gc.C) {
	rurl := router.MustNewResolvedURL("cs:~charmers/development/trusty/mysql-42", -1)
	err := s.store.AddCharmWithArchive(rurl, storetesting.Charms.CharmDir("mysql"))
	c.Assert(err, gc.IsNil)

	var entity *mongodoc.Entity
	err = s.store.DB.Entities().FindId(rurl.URL.String()).One(&entity)
	c.Assert(err, gc.IsNil)
	present, err := s.store.ES.HasDocument(s.TestIndex, typeName, s.store.ES.getID(entity.URL))
	c.Assert(err, gc.IsNil)
	c.Assert(present, gc.Equals, false)
}

func (s *StoreSearchSuite) TestNoExportDevelopment(c *gc.C) {
	rurl := router.MustNewResolvedURL("cs:~charmers/development/trusty/mysql-42", -1)
	err := s.store.AddCharmWithArchive(rurl, storetesting.Charms.CharmDir("mysql"))
	c.Assert(err, gc.IsNil)
	err = s.store.Publish(rurl, charm.DevelopmentChannel)
	c.Assert(err, gc.IsNil)

	var entity *mongodoc.Entity
	err = s.store.DB.Entities().FindId(rurl.URL.String()).One(&entity)
	c.Assert(err, gc.IsNil)
	present, err := s.store.ES.HasDocument(s.TestIndex, typeName, s.store.ES.getID(entity.URL))
	c.Assert(err, gc.IsNil)
	c.Assert(present, gc.Equals, false)
}

func (s *StoreSearchSuite) TestExportOnlyLatest(c *gc.C) {
	charmArchive := storetesting.Charms.CharmDir("wordpress")
	url := router.MustNewResolvedURL("cs:~charmers/precise/wordpress-24", -1)
	err := s.store.AddCharmWithArchive(url, charmArchive)
	c.Assert(err, gc.IsNil)
	err = s.store.Publish(url, StableChannel)
	c.Assert(err, gc.IsNil)
	var expected, old *mongodoc.Entity
	var actual json.RawMessage
	err = s.store.DB.Entities().FindId("cs:~charmers/precise/wordpress-23").One(&old)
	c.Assert(err, gc.IsNil)
	err = s.store.DB.Entities().FindId("cs:~charmers/precise/wordpress-24").One(&expected)
	c.Assert(err, gc.IsNil)
	err = s.store.ES.GetDocument(s.TestIndex, typeName, s.store.ES.getID(old.URL), &actual)
	c.Assert(err, gc.IsNil)
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
	charmArchive := storetesting.Charms.CharmDir("wordpress")
	url := router.MustNewResolvedURL("cs:~charmers/trusty/juju-gui-24", -1)
	err := s.store.AddCharmWithArchive(url, charmArchive)
	c.Assert(err, gc.IsNil)
	err = s.store.Publish(url, StableChannel)
	c.Assert(err, gc.IsNil)
	charmArchive = storetesting.Charms.CharmDir("multi-series")
	url = router.MustNewResolvedURL("cs:~charmers/juju-gui-25", -1)
	err = s.store.AddCharmWithArchive(url, charmArchive)
	c.Assert(err, gc.IsNil)
	err = s.store.Publish(url, StableChannel)
	c.Assert(err, gc.IsNil)
	var expected, old *mongodoc.Entity
	var actual json.RawMessage
	err = s.store.DB.Entities().FindId("cs:~charmers/trusty/juju-gui-24").One(&old)
	c.Assert(err, gc.IsNil)
	err = s.store.DB.Entities().FindId("cs:~charmers/juju-gui-25").One(&expected)
	c.Assert(err, gc.IsNil)
	err = s.store.ES.GetDocument(s.TestIndex, typeName, s.store.ES.getID(expected.URL), &actual)
	c.Assert(err, gc.IsNil)
	doc := SearchDoc{
		Entity:       expected,
		ReadACLs:     []string{"charmers"},
		Series:       expected.SupportedSeries,
		SingleSeries: false,
		AllSeries:    true,
	}
	c.Assert(string(actual), jc.JSONEquals, doc)
	err = s.store.ES.GetDocument(s.TestIndex, typeName, s.store.ES.getID(old.URL), &actual)
	c.Assert(err, gc.IsNil)
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
	err := s.store.DB.Entities().FindId("cs:~charmers/precise/wordpress-23").One(&entity)
	c.Assert(err, gc.IsNil)
	doc := SearchDoc{Entity: entity, TotalDownloads: 4000}
	err = s.store.ES.update(&doc)
	c.Assert(err, gc.IsNil)
	err = s.store.ES.GetDocument(s.TestIndex, typeName, s.store.ES.getID(entity.URL), &actual)
	c.Assert(err, gc.IsNil)
	c.Assert(string(actual), jc.JSONEquals, doc)
}

func (s *StoreSearchSuite) addCharmsToStore(c *gc.C) {
	for name, ent := range exportTestCharms {
		charmArchive := storetesting.Charms.CharmDir(name)
		cats := strings.Split(name, "-")
		charmArchive.Meta().Categories = cats
		tags := make([]string, len(cats))
		for i, s := range cats {
			tags[i] = s + "TAG"
		}
		charmArchive.Meta().Tags = tags
		url := EntityResolvedURL(ent)
		err := s.store.AddCharmWithArchive(url, charmArchive)
		c.Assert(err, gc.IsNil)
		err = s.store.Publish(url, StableChannel)
		c.Assert(err, gc.IsNil)
		for i := 0; i < charmDownloadCounts[name]; i++ {
			err := s.store.IncrementDownloadCounts(url)
			c.Assert(err, gc.IsNil)
		}
		if ent.URL.Name == "riak" {
			continue
		}
		err = s.store.SetPerms(ent.URL, "read", ent.URL.User, params.Everyone)
		c.Assert(err, gc.IsNil)
		err = s.store.UpdateSearchBaseURL(mongodoc.BaseURL(ent.URL))
		c.Assert(err, gc.IsNil)
	}
	for name, ent := range exportTestBundles {
		bundleArchive := storetesting.Charms.BundleDir(name)
		bundleArchive.Data().Tags = strings.Split(name, "-")
		url := EntityResolvedURL(ent)
		err := s.store.AddBundleWithArchive(url, bundleArchive)
		c.Assert(err, gc.IsNil)
		err = s.store.Publish(url, StableChannel)
		c.Assert(err, gc.IsNil)
		for i := 0; i < charmDownloadCounts[name]; i++ {
			err := s.store.IncrementDownloadCounts(url)
			c.Assert(err, gc.IsNil)
		}
		err = s.store.SetPerms(ent.URL, "read", ent.URL.User, params.Everyone)
		c.Assert(err, gc.IsNil)
		err = s.store.UpdateSearchBaseURL(mongodoc.BaseURL(ent.URL))
		c.Assert(err, gc.IsNil)
	}
	s.store.pool.statsCache.EvictAll()
	err := s.store.syncSearch()
	c.Assert(err, gc.IsNil)
}

var searchTests = []struct {
	about     string
	sp        SearchParams
	results   []*mongodoc.Entity
	totalDiff int // len(results) + totalDiff = expected total
}{
	{
		about: "basic text search",
		sp: SearchParams{
			Text: "wordpress",
		},
		results: []*mongodoc.Entity{
			exportTestCharms["wordpress"],
			exportTestBundles["wordpress-simple"],
		},
	}, {
		about: "blank text search",
		sp: SearchParams{
			Text: "",
		},
		results: []*mongodoc.Entity{
			exportTestCharms["wordpress"],
			exportTestCharms["mysql"],
			exportTestCharms["varnish"],
			exportTestBundles["wordpress-simple"],
		},
	}, {
		about: "autocomplete search",
		sp: SearchParams{
			Text:         "word",
			AutoComplete: true,
		},
		results: []*mongodoc.Entity{
			exportTestCharms["wordpress"],
			exportTestBundles["wordpress-simple"],
		},
	}, {
		about: "description filter search",
		sp: SearchParams{
			Text: "",
			Filters: map[string][]string{
				"description": {"blog"},
			},
		},
		results: []*mongodoc.Entity{
			exportTestCharms["wordpress"],
		},
	}, {
		about: "name filter search",
		sp: SearchParams{
			Text: "",
			Filters: map[string][]string{
				"name": {"wordpress"},
			},
		},
		results: []*mongodoc.Entity{
			exportTestCharms["wordpress"],
		},
	}, {
		about: "owner filter search",
		sp: SearchParams{
			Text: "",
			Filters: map[string][]string{
				"owner": {"foo"},
			},
		},
		results: []*mongodoc.Entity{
			exportTestCharms["varnish"],
		},
	}, {
		about: "provides filter search",
		sp: SearchParams{
			Text: "",
			Filters: map[string][]string{
				"provides": {"mysql"},
			},
		},
		results: []*mongodoc.Entity{
			exportTestCharms["mysql"],
		},
	}, {
		about: "requires filter search",
		sp: SearchParams{
			Text: "",
			Filters: map[string][]string{
				"requires": {"mysql"},
			},
		},
		results: []*mongodoc.Entity{
			exportTestCharms["wordpress"],
		},
	}, {
		about: "series filter search",
		sp: SearchParams{
			Text: "",
			Filters: map[string][]string{
				"series": {"trusty"},
			},
		},
		results: []*mongodoc.Entity{
			exportTestCharms["mysql"],
			exportTestCharms["varnish"],
		},
	}, {
		about: "summary filter search",
		sp: SearchParams{
			Text: "",
			Filters: map[string][]string{
				"summary": {"Database engine"},
			},
		},
		results: []*mongodoc.Entity{
			exportTestCharms["mysql"],
			exportTestCharms["varnish"],
		},
	}, {
		about: "tags filter search",
		sp: SearchParams{
			Text: "",
			Filters: map[string][]string{
				"tags": {"wordpress"},
			},
		},
		results: []*mongodoc.Entity{
			exportTestCharms["wordpress"],
			exportTestBundles["wordpress-simple"],
		},
	}, {
		about: "bundle type filter search",
		sp: SearchParams{
			Text: "",
			Filters: map[string][]string{
				"type": {"bundle"},
			},
		},
		results: []*mongodoc.Entity{
			exportTestBundles["wordpress-simple"],
		},
	}, {
		about: "charm type filter search",
		sp: SearchParams{
			Text: "",
			Filters: map[string][]string{
				"type": {"charm"},
			},
		},
		results: []*mongodoc.Entity{
			exportTestCharms["wordpress"],
			exportTestCharms["mysql"],
			exportTestCharms["varnish"],
		},
	}, {
		about: "charm & bundle type filter search",
		sp: SearchParams{
			Text: "",
			Filters: map[string][]string{
				"type": {"charm", "bundle"},
			},
		},
		results: []*mongodoc.Entity{
			exportTestCharms["wordpress"],
			exportTestCharms["mysql"],
			exportTestCharms["varnish"],
			exportTestBundles["wordpress-simple"],
		},
	}, {
		about: "invalid filter search",
		sp: SearchParams{
			Text: "",
			Filters: map[string][]string{
				"no such filter": {"foo"},
			},
		},
		results: []*mongodoc.Entity{
			exportTestCharms["wordpress"],
			exportTestCharms["mysql"],
			exportTestCharms["varnish"],
			exportTestBundles["wordpress-simple"],
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
		results: []*mongodoc.Entity{
			exportTestCharms["wordpress"],
			exportTestCharms["mysql"],
			exportTestCharms["varnish"],
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
		results: []*mongodoc.Entity{
			exportTestCharms["riak"],
			exportTestCharms["wordpress"],
			exportTestCharms["mysql"],
			exportTestCharms["varnish"],
			exportTestBundles["wordpress-simple"],
		},
	}, {
		about: "admin search",
		sp: SearchParams{
			Admin: true,
		},
		results: []*mongodoc.Entity{
			exportTestCharms["riak"],
			exportTestCharms["wordpress"],
			exportTestCharms["mysql"],
			exportTestCharms["varnish"],
			exportTestBundles["wordpress-simple"],
		},
	}, {
		about: "charm tags filter search",
		sp: SearchParams{
			Text: "",
			Filters: map[string][]string{
				"tags": {"wordpressTAG"},
			},
		},
		results: []*mongodoc.Entity{
			exportTestCharms["wordpress"],
		},
	}, {
		about: "blank owner filter search",
		sp: SearchParams{
			Text: "",
			Filters: map[string][]string{
				"owner": {""},
			},
		},
		results: []*mongodoc.Entity{
			exportTestCharms["wordpress"],
			exportTestCharms["mysql"],
			exportTestBundles["wordpress-simple"],
		},
	}, {
		about: "promulgated search",
		sp: SearchParams{
			Text: "",
			Filters: map[string][]string{
				"promulgated": {"1"},
			},
		},
		results: []*mongodoc.Entity{
			exportTestCharms["wordpress"],
			exportTestCharms["mysql"],
			exportTestBundles["wordpress-simple"],
		},
	}, {
		about: "not promulgated search",
		sp: SearchParams{
			Text: "",
			Filters: map[string][]string{
				"promulgated": {"0"},
			},
		},
		results: []*mongodoc.Entity{
			exportTestCharms["varnish"],
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
		results: []*mongodoc.Entity{
			exportTestCharms["mysql"],
		},
	},
}

func (s *StoreSearchSuite) TestSearches(c *gc.C) {
	s.store.ES.Database.RefreshIndex(s.TestIndex)
	for i, test := range searchTests {
		c.Logf("test %d: %s", i, test.about)
		res, err := s.store.Search(test.sp)
		c.Assert(err, gc.IsNil)
		c.Logf("results: %v", res.Results)
		sort.Sort(resolvedURLsByString(res.Results))
		sort.Sort(resolvedURLsByString(test.results))
		c.Check(res.Results, jc.DeepEquals, test.results)
		c.Check(res.Total, gc.Equals, len(test.results)+test.totalDiff)
	}
}

type resolvedURLsByString []*mongodoc.Entity

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
	c.Assert(err, gc.IsNil)
	sp := SearchParams{
		Text: "wordpress",
		Skip: 1,
	}
	res, err := s.store.Search(sp)
	c.Assert(err, gc.IsNil)
	c.Assert(res.Results, gc.HasLen, 1)
	c.Assert(res.Total, gc.Equals, 2)
}

func (s *StoreSearchSuite) TestLimitTestSearch(c *gc.C) {
	err := s.store.ES.Database.RefreshIndex(s.TestIndex)
	c.Assert(err, gc.IsNil)
	sp := SearchParams{
		Text:  "wordpress",
		Limit: 1,
	}
	res, err := s.store.Search(sp)
	c.Assert(err, gc.IsNil)
	c.Assert(res.Results, gc.HasLen, 1)
}

func (s *StoreSearchSuite) TestPromulgatedRank(c *gc.C) {
	charmArchive := storetesting.Charms.CharmDir("varnish")
	ent := newEntity("cs:~charmers/trusty/varnish-1", 1)
	url := EntityResolvedURL(ent)
	s.store.AddCharmWithArchive(url, charmArchive)
	err := s.store.Publish(url, StableChannel)
	c.Assert(err, gc.IsNil)
	err = s.store.SetPerms(ent.URL, "read", ent.URL.User, params.Everyone)
	c.Assert(err, gc.IsNil)
	err = s.store.UpdateSearchBaseURL(mongodoc.BaseURL(ent.URL))
	c.Assert(err, gc.IsNil)
	s.store.ES.Database.RefreshIndex(s.TestIndex)
	sp := SearchParams{
		Filters: map[string][]string{
			"name": {"varnish"},
		},
	}
	res, err := s.store.Search(sp)
	c.Assert(err, gc.IsNil)
	c.Logf("results: %#v", res.Results)
	c.Assert(res.Results, jc.DeepEquals, []*mongodoc.Entity{
		ent,
		exportTestCharms["varnish"],
	})
}

func (s *StoreSearchSuite) TestSorting(c *gc.C) {
	s.store.ES.Database.RefreshIndex(s.TestIndex)
	tests := []struct {
		about     string
		sortQuery string
		results   []*mongodoc.Entity
	}{{
		about:     "name ascending",
		sortQuery: "name",
		results: []*mongodoc.Entity{
			exportTestCharms["mysql"],
			exportTestCharms["varnish"],
			exportTestCharms["wordpress"],
			exportTestBundles["wordpress-simple"],
		},
	}, {
		about:     "name descending",
		sortQuery: "-name",
		results: []*mongodoc.Entity{
			exportTestBundles["wordpress-simple"],
			exportTestCharms["wordpress"],
			exportTestCharms["varnish"],
			exportTestCharms["mysql"],
		},
	}, {
		about:     "series ascending",
		sortQuery: "series,name",
		results: []*mongodoc.Entity{
			exportTestBundles["wordpress-simple"],
			exportTestCharms["wordpress"],
			exportTestCharms["mysql"],
			exportTestCharms["varnish"],
		},
	}, {
		about:     "series descending",
		sortQuery: "-series,name",
		results: []*mongodoc.Entity{
			exportTestCharms["mysql"],
			exportTestCharms["varnish"],
			exportTestCharms["wordpress"],
			exportTestBundles["wordpress-simple"],
		},
	}, {
		about:     "owner ascending",
		sortQuery: "owner,name",
		results: []*mongodoc.Entity{
			exportTestCharms["wordpress"],
			exportTestBundles["wordpress-simple"],
			exportTestCharms["varnish"],
			exportTestCharms["mysql"],
		},
	}, {
		about:     "owner descending",
		sortQuery: "-owner,name",
		results: []*mongodoc.Entity{
			exportTestCharms["mysql"],
			exportTestCharms["varnish"],
			exportTestCharms["wordpress"],
			exportTestBundles["wordpress-simple"],
		},
	}, {
		about:     "downloads ascending",
		sortQuery: "downloads",
		results: []*mongodoc.Entity{
			exportTestCharms["wordpress"],
			exportTestBundles["wordpress-simple"],
			exportTestCharms["mysql"],
			exportTestCharms["varnish"],
		},
	}, {
		about:     "downloads descending",
		sortQuery: "-downloads",
		results: []*mongodoc.Entity{
			exportTestCharms["varnish"],
			exportTestCharms["mysql"],
			exportTestBundles["wordpress-simple"],
			exportTestCharms["wordpress"],
		},
	}}
	for i, test := range tests {
		c.Logf("test %d. %s", i, test.about)
		var sp SearchParams
		err := sp.ParseSortFields(test.sortQuery)
		c.Assert(err, gc.IsNil)
		res, err := s.store.Search(sp)
		c.Assert(err, gc.IsNil)
		c.Assert(res.Results, jc.DeepEquals, test.results)
		c.Assert(res.Total, gc.Equals, len(test.results))
	}
}

func (s *StoreSearchSuite) TestBoosting(c *gc.C) {
	s.store.ES.Database.RefreshIndex(s.TestIndex)
	var sp SearchParams
	res, err := s.store.Search(sp)
	c.Assert(err, gc.IsNil)
	c.Assert(res.Results, gc.HasLen, 4)
	c.Logf("results: %#v", res.Results)
	c.Assert(res.Results, jc.DeepEquals, []*mongodoc.Entity{
		exportTestBundles["wordpress-simple"],
		exportTestCharms["mysql"],
		exportTestCharms["wordpress"],
		exportTestCharms["varnish"],
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
	indexes, err = s.ES.ListIndexesForAlias(s.store.ES.Index)
	c.Assert(err, gc.Equals, nil)
	c.Assert(indexes, gc.HasLen, 1)
	wg.Wait()
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
	charmArchive := storetesting.Charms.CharmDir("multi-series")
	url := router.MustNewResolvedURL("cs:~charmers/juju-gui-25", -1)
	err := s.store.AddCharmWithArchive(url, charmArchive)
	c.Assert(err, gc.IsNil)
	err = s.store.Publish(url, StableChannel)
	c.Assert(err, gc.IsNil)
	err = s.store.SetPerms(&url.URL, "read", url.URL.User, params.Everyone)
	c.Assert(err, gc.IsNil)
	err = s.store.UpdateSearch(url)
	c.Assert(err, gc.IsNil)
	s.store.ES.Database.RefreshIndex(s.TestIndex)
	filterTests := []struct {
		series   string
		notFound bool
	}{{
		series: "trusty",
	}, {
		series: "vivid",
	}, {
		series:   "sauch",
		notFound: true,
	}}
	for i, test := range filterTests {
		c.Logf("%d. %s", i, test.series)
		res, err := s.store.Search(SearchParams{
			Filters: map[string][]string{
				"name":   []string{"juju-gui"},
				"series": []string{test.series},
			},
		})
		c.Assert(err, gc.IsNil)
		if test.notFound {
			c.Assert(res.Results, gc.HasLen, 0)
			continue
		}
		c.Assert(res.Results, gc.HasLen, 1)
		c.Assert(res.Results[0].URL.String(), gc.Equals, url.String())
	}
}

func (s *StoreSearchSuite) TestMultiSeriesCharmSortsSeriesCorrectly(c *gc.C) {
	charmArchive := storetesting.Charms.CharmDir("multi-series")
	url := router.MustNewResolvedURL("cs:~charmers/juju-gui-25", -1)
	err := s.store.AddCharmWithArchive(url, charmArchive)
	c.Assert(err, gc.IsNil)
	err = s.store.Publish(url, StableChannel)
	c.Assert(err, gc.IsNil)
	err = s.store.SetPerms(&url.URL, "read", url.URL.User, params.Everyone)
	c.Assert(err, gc.IsNil)
	err = s.store.UpdateSearch(url)
	c.Assert(err, gc.IsNil)
	s.store.ES.Database.RefreshIndex(s.TestIndex)
	var sp SearchParams
	sp.ParseSortFields("-series", "owner")
	res, err := s.store.Search(sp)
	c.Assert(err, gc.IsNil)
	c.Assert(res.Results, jc.DeepEquals, []*mongodoc.Entity{
		newEntity("cs:~charmers/juju-gui-25", -1, "trusty", "utopic", "vivid", "wily"),
		newEntity("cs:~foo/trusty/varnish-1", -1),
		newEntity("cs:~openstack-charmers/trusty/mysql-7", 7),
		newEntity("cs:~charmers/precise/wordpress-23", 23),
		newEntity("cs:~charmers/bundle/wordpress-simple-4", 4),
	})
}
