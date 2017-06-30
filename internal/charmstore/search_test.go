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

	"gopkg.in/juju/charmstore.v5-unstable/elasticsearch"
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
	s.index = SearchIndex{s.ES, s.TestIndex}
	s.ES.RefreshIndex(".versions")
	pool, err := NewPool(s.Session.DB("foo"), &s.index, nil, ServerParams{})
	c.Assert(err, gc.Equals, nil)
	s.pool = pool
	s.store = pool.Store()
	s.addEntities(c)
	c.Assert(err, gc.Equals, nil)
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

type searchEntity struct {
	entity     *mongodoc.Entity
	charmMeta  *charm.Meta
	bundleData *charm.BundleData
	acl        []string
	downloads  int
}

var searchEntities = map[string]searchEntity{
	"wordpress": {
		entity: newEntity("cs:~charmers/precise/wordpress-23", 23),
		charmMeta: &charm.Meta{
			Description: "blog",
			Requires: map[string]charm.Relation{
				"mysql": {
					Name:      "mysql",
					Interface: "mysql",
					Scope:     charm.ScopeGlobal,
				},
			},
			Categories: []string{"wordpress", "wordpressCAT"},
			Tags:       []string{"wordpressTAG"},
		},
		acl: []string{params.Everyone},
	},
	"mysql": {
		entity: newEntity("cs:~openstack-charmers/xenial/mysql-7", 7),
		charmMeta: &charm.Meta{
			Summary: "Database Engine",
			Provides: map[string]charm.Relation{
				"mysql": {
					Name:      "mysql",
					Interface: "mysql",
					Scope:     charm.ScopeGlobal,
				},
			},
			Categories: []string{"mysql"},
			Tags:       []string{"mysqlTAG"},
		},
		acl:       []string{params.Everyone},
		downloads: 3,
	},
	"varnish": {
		entity: newEntity("cs:~foo/xenial/varnish-1", -1),
		charmMeta: &charm.Meta{
			Summary:    "Database Engine",
			Categories: []string{"varnish"},
			Tags:       []string{"varnishTAG"},
		},
		acl:       []string{params.Everyone},
		downloads: 5,
	},
	"riak": {
		entity: newEntity("cs:~charmers/xenial/riak-67", 67),
		charmMeta: &charm.Meta{
			Categories: []string{"riak"},
			Tags:       []string{"riakTAG"},
		},
		acl: []string{"charmers"},
	},
	"wordpress-simple": {
		entity: newEntity("cs:~charmers/bundle/wordpress-simple-4", 4),
		bundleData: &charm.BundleData{
			Applications: map[string]*charm.ApplicationSpec{
				"wordpress": {
					Charm: "wordpress",
				},
			},
			Tags: []string{"wordpress"},
		},
		acl:       []string{params.Everyone},
		downloads: 1,
	},
	// Note: "squid-forwardproxy" shares a trigram "dpr" with "wordpress".
	"squid-forwardproxy": {
		entity:    newEntity("cs:~charmers/yakkety/squid-forwardproxy-3", 3),
		charmMeta: &charm.Meta{},
		acl:       []string{params.Everyone},
		downloads: 2,
	},
	// Note: "cloud-controller-worker-v2" shares a trigram "wor" with "wordpress".

	"cloud-controller-worker-v2": {
		entity:    newEntity("cs:~cf-charmers/trusty/cloud-controller-worker-v2-7", -1),
		charmMeta: &charm.Meta{},
		acl:       []string{params.Everyone},
		downloads: 4,
	},
}

func (s *StoreSearchSuite) addEntities(c *gc.C) {
	for _, ent := range searchEntities {
		if ent.charmMeta == nil {
			continue
		}
		addCharmForSearch(
			c,
			s.store,
			EntityResolvedURL(ent.entity),
			storetesting.NewCharm(ent.charmMeta),
			ent.acl,
			ent.downloads,
		)
	}
	for _, ent := range searchEntities {
		if ent.bundleData == nil {
			continue
		}
		addBundleForSearch(
			c,
			s.store,
			EntityResolvedURL(ent.entity),
			storetesting.NewBundle(ent.bundleData),
			ent.acl,
			ent.downloads,
		)
	}
	s.store.pool.statsCache.EvictAll()
	err := s.store.syncSearch()
	c.Assert(err, gc.Equals, nil)
}

func (s *StoreSearchSuite) TestSuccessfulExport(c *gc.C) {
	s.store.pool.statsCache.EvictAll()
	for _, ent := range searchEntities {
		entity, err := s.store.FindEntity(EntityResolvedURL(ent.entity), nil)
		c.Assert(err, gc.Equals, nil)
		var actual json.RawMessage
		err = s.store.ES.GetDocument(s.TestIndex, typeName, s.store.ES.getID(entity.URL), &actual)
		c.Assert(err, gc.Equals, nil)
		series := entity.SupportedSeries
		if ent.bundleData != nil {
			series = []string{"bundle"}
		}
		doc := SearchDoc{
			Entity:         entity,
			TotalDownloads: int64(ent.downloads),
			ReadACLs:       ent.acl,
			Series:         series,
			AllSeries:      true,
			SingleSeries:   true,
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
	err := s.store.DB.Entities().FindId("cs:~openstack-charmers/xenial/mysql-7").One(&entity)
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
	url := router.MustNewResolvedURL("cs:~charmers/precise/wordpress-24", -1)
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
	err := s.store.DB.Entities().FindId("cs:~charmers/precise/wordpress-23").One(&old)
	c.Assert(err, gc.Equals, nil)
	err = s.store.DB.Entities().FindId("cs:~charmers/precise/wordpress-24").One(&expected)
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
	url := router.MustNewResolvedURL("cs:~charmers/xenial/juju-gui-24", -1)
	addCharmForSearch(
		c,
		s.store,
		url,
		charmArchive,
		[]string{"charmers"},
		0,
	)
	charmArchive = storetesting.NewCharm(storetesting.MetaWithSupportedSeries(nil, "trusty", "xenial", "utopic", "vivid", "wily", "yakkety"))
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
	err := s.store.DB.Entities().FindId("cs:~charmers/xenial/juju-gui-24").One(&old)
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
	err := s.store.DB.Entities().FindId("cs:~charmers/precise/wordpress-23").One(&entity)
	c.Assert(err, gc.Equals, nil)
	doc := SearchDoc{Entity: entity, TotalDownloads: 4000}
	err = s.store.ES.update(&doc)
	c.Assert(err, gc.Equals, nil)
	err = s.store.ES.GetDocument(s.TestIndex, typeName, s.store.ES.getID(entity.URL), &actual)
	c.Assert(err, gc.Equals, nil)
	c.Assert(string(actual), jc.JSONEquals, doc)
}

func (s *StoreSearchSuite) TestDeleteDocument(c *gc.C) {
	var entity *mongodoc.Entity
	var actual json.RawMessage
	err := s.store.DB.Entities().FindId("cs:~charmers/precise/wordpress-23").One(&entity)
	c.Assert(err, gc.Equals, nil)
	err = s.store.ES.GetDocument(s.TestIndex, typeName, s.store.ES.getID(entity.URL), &actual)
	c.Assert(err, gc.Equals, nil)
	err = s.store.ES.delete(entity)
	c.Assert(err, gc.Equals, nil)
	err = s.store.ES.GetDocument(s.TestIndex, typeName, s.store.ES.getID(entity.URL), &actual)
	c.Assert(err, gc.Equals, elasticsearch.ErrNotFound)
}

var searchTests = []struct {
	about     string
	sp        SearchParams
	results   Entities
	totalDiff int // len(results) + totalDiff = expected total
}{
	{
		about: "basic text search",
		sp: SearchParams{
			Text: "wordpress",
		},
		results: Entities{
			searchEntities["wordpress"].entity,
			searchEntities["wordpress-simple"].entity,
		},
	}, {
		about: "blank text search",
		sp: SearchParams{
			Text: "",
		},
		results: Entities{
			searchEntities["cloud-controller-worker-v2"].entity,
			searchEntities["wordpress"].entity,
			searchEntities["mysql"].entity,
			searchEntities["varnish"].entity,
			searchEntities["squid-forwardproxy"].entity,
			searchEntities["wordpress-simple"].entity,
		},
	}, {
		about: "autocomplete search",
		sp: SearchParams{
			Text:         "word",
			AutoComplete: true,
		},
		results: Entities{
			searchEntities["wordpress"].entity,
			searchEntities["wordpress-simple"].entity,
		},
	}, {
		about: "autocomplete case insensitive",
		sp: SearchParams{
			Text:         "woRd",
			AutoComplete: true,
		},
		results: Entities{
			searchEntities["wordpress"].entity,
			searchEntities["wordpress-simple"].entity,
		},
	}, {
		about: "autocomplete end of word",
		sp: SearchParams{
			Text:         "PRESS",
			AutoComplete: true,
		},
		results: Entities{
			searchEntities["wordpress"].entity,
			searchEntities["wordpress-simple"].entity,
		},
	}, {
		about: "non-matching autocomplete search",
		sp: SearchParams{
			Text:         "worm",
			AutoComplete: true,
		},
		results: Entities{},
	}, {
		about: "autocomplete with hyphen - match",
		sp: SearchParams{
			Text:         "squid-f",
			AutoComplete: true,
		},
		results: Entities{
			searchEntities["squid-forwardproxy"].entity,
		},
	}, {
		about: "autocomplete with hyphen - no match",
		sp: SearchParams{
			Text:         "squid-g",
			AutoComplete: true,
		},
		results: Entities{},
	}, {
		about: "description filter search",
		sp: SearchParams{
			Text: "",
			Filters: map[string][]string{
				"description": {"blog"},
			},
		},
		results: Entities{
			searchEntities["wordpress"].entity,
		},
	}, {
		about: "name filter search",
		sp: SearchParams{
			Text: "",
			Filters: map[string][]string{
				"name": {"wordpress"},
			},
		},
		results: Entities{
			searchEntities["wordpress"].entity,
		},
	}, {
		about: "owner filter search",
		sp: SearchParams{
			Text: "",
			Filters: map[string][]string{
				"owner": {"foo"},
			},
		},
		results: Entities{
			searchEntities["varnish"].entity,
		},
	}, {
		about: "provides filter search",
		sp: SearchParams{
			Text: "",
			Filters: map[string][]string{
				"provides": {"mysql"},
			},
		},
		results: Entities{
			searchEntities["mysql"].entity,
		},
	}, {
		about: "requires filter search",
		sp: SearchParams{
			Text: "",
			Filters: map[string][]string{
				"requires": {"mysql"},
			},
		},
		results: Entities{
			searchEntities["wordpress"].entity,
		},
	}, {
		about: "series filter search",
		sp: SearchParams{
			Text: "",
			Filters: map[string][]string{
				"series": {"xenial"},
			},
		},
		results: Entities{
			searchEntities["mysql"].entity,
			searchEntities["varnish"].entity,
		},
	}, {
		about: "summary filter search",
		sp: SearchParams{
			Text: "",
			Filters: map[string][]string{
				"summary": {"Database engine"},
			},
		},
		results: Entities{
			searchEntities["mysql"].entity,
			searchEntities["varnish"].entity,
		},
	}, {
		about: "tags filter search",
		sp: SearchParams{
			Text: "",
			Filters: map[string][]string{
				"tags": {"wordpress"},
			},
		},
		results: Entities{
			searchEntities["wordpress"].entity,
			searchEntities["wordpress-simple"].entity,
		},
	}, {
		about: "bundle type filter search",
		sp: SearchParams{
			Text: "",
			Filters: map[string][]string{
				"type": {"bundle"},
			},
		},
		results: Entities{
			searchEntities["wordpress-simple"].entity,
		},
	}, {
		about: "charm type filter search",
		sp: SearchParams{
			Text: "",
			Filters: map[string][]string{
				"type": {"charm"},
			},
		},
		results: Entities{
			searchEntities["cloud-controller-worker-v2"].entity,
			searchEntities["wordpress"].entity,
			searchEntities["mysql"].entity,
			searchEntities["varnish"].entity,
			searchEntities["squid-forwardproxy"].entity,
		},
	}, {
		about: "charm & bundle type filter search",
		sp: SearchParams{
			Text: "",
			Filters: map[string][]string{
				"type": {"charm", "bundle"},
			},
		},
		results: Entities{
			searchEntities["cloud-controller-worker-v2"].entity,
			searchEntities["wordpress"].entity,
			searchEntities["mysql"].entity,
			searchEntities["varnish"].entity,
			searchEntities["squid-forwardproxy"].entity,
			searchEntities["wordpress-simple"].entity,
		},
	}, {
		about: "invalid filter search",
		sp: SearchParams{
			Text: "",
			Filters: map[string][]string{
				"no such filter": {"foo"},
			},
		},
		results: Entities{
			searchEntities["cloud-controller-worker-v2"].entity,
			searchEntities["wordpress"].entity,
			searchEntities["mysql"].entity,
			searchEntities["varnish"].entity,
			searchEntities["squid-forwardproxy"].entity,
			searchEntities["wordpress-simple"].entity,
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
		results: Entities{
			searchEntities["cloud-controller-worker-v2"].entity,
			searchEntities["wordpress"].entity,
			searchEntities["mysql"].entity,
			searchEntities["varnish"].entity,
			searchEntities["squid-forwardproxy"].entity,
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
		results: Entities{
			searchEntities["riak"].entity,
			searchEntities["cloud-controller-worker-v2"].entity,
			searchEntities["wordpress"].entity,
			searchEntities["mysql"].entity,
			searchEntities["varnish"].entity,
			searchEntities["squid-forwardproxy"].entity,
			searchEntities["wordpress-simple"].entity,
		},
	}, {
		about: "admin search",
		sp: SearchParams{
			Admin: true,
		},
		results: Entities{
			searchEntities["riak"].entity,
			searchEntities["cloud-controller-worker-v2"].entity,
			searchEntities["wordpress"].entity,
			searchEntities["mysql"].entity,
			searchEntities["varnish"].entity,
			searchEntities["squid-forwardproxy"].entity,
			searchEntities["wordpress-simple"].entity,
		},
	}, {
		about: "charm tags filter search",
		sp: SearchParams{
			Text: "",
			Filters: map[string][]string{
				"tags": {"wordpressTAG"},
			},
		},
		results: Entities{
			searchEntities["wordpress"].entity,
		},
	}, {
		about: "blank owner filter search",
		sp: SearchParams{
			Text: "",
			Filters: map[string][]string{
				"owner": {""},
			},
		},
		results: Entities{
			searchEntities["wordpress"].entity,
			searchEntities["mysql"].entity,
			searchEntities["squid-forwardproxy"].entity,
			searchEntities["wordpress-simple"].entity,
		},
	}, {
		about: "promulgated search",
		sp: SearchParams{
			Text: "",
			Filters: map[string][]string{
				"promulgated": {"1"},
			},
		},
		results: Entities{
			searchEntities["wordpress"].entity,
			searchEntities["mysql"].entity,
			searchEntities["squid-forwardproxy"].entity,
			searchEntities["wordpress-simple"].entity,
		},
	}, {
		about: "not promulgated search",
		sp: SearchParams{
			Text: "",
			Filters: map[string][]string{
				"promulgated": {"0"},
			},
		},
		results: Entities{
			searchEntities["cloud-controller-worker-v2"].entity,
			searchEntities["varnish"].entity,
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
		results: Entities{
			searchEntities["mysql"].entity,
		},
	}, {
		about: "name search",
		sp: SearchParams{
			Text: "wordpress",
		},
		results: Entities{
			searchEntities["wordpress"].entity,
			searchEntities["wordpress-simple"].entity,
		},
	}, {
		about: "case insensitive search",
		sp: SearchParams{
			Text: "WORDPRESS",
		},
		results: Entities{
			searchEntities["wordpress"].entity,
			searchEntities["wordpress-simple"].entity,
		},
	}, {
		about: "case insensitive search on tags",
		sp: SearchParams{
			Text: "WORDPRESSTAG",
		},
		results: Entities{
			searchEntities["wordpress"].entity,
		},
	}, {
		about: "case insensitive search on categories",
		sp: SearchParams{
			Text: "WORDPRESSCAT",
		},
		results: Entities{
			searchEntities["wordpress"].entity,
		},
	}, {
		about: "autocomplete with spaces",
		sp: SearchParams{
			Text:         "wordpress simple",
			AutoComplete: true,
		},
		results: Entities{
			searchEntities["wordpress-simple"].entity,
		},
	}, {
		about: "autocomplete with spaces, reversed",
		sp: SearchParams{
			Text:         "simple wordpress",
			AutoComplete: true,
		},
		results: Entities{
			searchEntities["wordpress-simple"].entity,
		},
	},
}

func (s *StoreSearchSuite) TestSearches(c *gc.C) {
	s.store.ES.Database.RefreshIndex(s.TestIndex)
	for i, test := range searchTests {
		c.Logf("test %d: %s", i, test.about)
		res, err := s.store.Search(test.sp)
		c.Assert(err, gc.Equals, nil)
		sort.Sort(resolvedURLsByString(res.Results))
		sort.Sort(resolvedURLsByString(test.results))
		c.Check(Entities(res.Results), jc.DeepEquals, test.results)
		c.Check(res.Total, gc.Equals, len(test.results)+test.totalDiff)
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
	res, err := s.store.Search(sp)
	c.Assert(err, gc.Equals, nil)
	c.Assert(res.Results, gc.HasLen, 1)
	c.Assert(res.Total, gc.Equals, 2)
}

func (s *StoreSearchSuite) TestLimitTestSearch(c *gc.C) {
	err := s.store.ES.Database.RefreshIndex(s.TestIndex)
	c.Assert(err, gc.Equals, nil)
	sp := SearchParams{
		Text:  "wordpress",
		Limit: 1,
	}
	res, err := s.store.Search(sp)
	c.Assert(err, gc.Equals, nil)
	c.Assert(res.Results, gc.HasLen, 1)
}

func (s *StoreSearchSuite) TestPromulgatedRank(c *gc.C) {
	charmArchive := storetesting.NewCharm(nil)
	ent := newEntity("cs:~charmers/xenial/varnish-1", 1)
	addCharmForSearch(
		c,
		s.store,
		EntityResolvedURL(ent),
		charmArchive,
		[]string{ent.URL.User, params.Everyone},
		0,
	)
	s.store.ES.Database.RefreshIndex(s.TestIndex)
	sp := SearchParams{
		Filters: map[string][]string{
			"name": {"varnish"},
		},
	}
	res, err := s.store.Search(sp)
	c.Assert(err, gc.Equals, nil)
	c.Assert(Entities(res.Results), jc.DeepEquals, Entities{
		ent,
		searchEntities["varnish"].entity,
	})
}

func (s *StoreSearchSuite) TestSorting(c *gc.C) {
	s.store.ES.Database.RefreshIndex(s.TestIndex)
	tests := []struct {
		about     string
		sortQuery string
		results   Entities
	}{{
		about:     "name ascending",
		sortQuery: "name",
		results: Entities{
			searchEntities["cloud-controller-worker-v2"].entity,
			searchEntities["mysql"].entity,
			searchEntities["squid-forwardproxy"].entity,
			searchEntities["varnish"].entity,
			searchEntities["wordpress"].entity,
			searchEntities["wordpress-simple"].entity,
		},
	}, {
		about:     "name descending",
		sortQuery: "-name",
		results: Entities{
			searchEntities["wordpress-simple"].entity,
			searchEntities["wordpress"].entity,
			searchEntities["varnish"].entity,
			searchEntities["squid-forwardproxy"].entity,
			searchEntities["mysql"].entity,
			searchEntities["cloud-controller-worker-v2"].entity,
		},
	}, {
		about:     "series ascending",
		sortQuery: "series,name",
		results: Entities{
			searchEntities["wordpress-simple"].entity,
			searchEntities["wordpress"].entity,
			searchEntities["cloud-controller-worker-v2"].entity,
			searchEntities["mysql"].entity,
			searchEntities["varnish"].entity,
			searchEntities["squid-forwardproxy"].entity,
		},
	}, {
		about:     "series descending",
		sortQuery: "-series,name",
		results: Entities{
			searchEntities["squid-forwardproxy"].entity,
			searchEntities["mysql"].entity,
			searchEntities["varnish"].entity,
			searchEntities["cloud-controller-worker-v2"].entity,
			searchEntities["wordpress"].entity,
			searchEntities["wordpress-simple"].entity,
		},
	}, {
		about:     "owner ascending",
		sortQuery: "owner,name",
		results: Entities{
			searchEntities["cloud-controller-worker-v2"].entity,
			searchEntities["squid-forwardproxy"].entity,
			searchEntities["wordpress"].entity,
			searchEntities["wordpress-simple"].entity,
			searchEntities["varnish"].entity,
			searchEntities["mysql"].entity,
		},
	}, {
		about:     "owner descending",
		sortQuery: "-owner,name",
		results: Entities{
			searchEntities["mysql"].entity,
			searchEntities["varnish"].entity,
			searchEntities["squid-forwardproxy"].entity,
			searchEntities["wordpress"].entity,
			searchEntities["wordpress-simple"].entity,
			searchEntities["cloud-controller-worker-v2"].entity,
		},
	}, {
		about:     "downloads ascending",
		sortQuery: "downloads",
		results: Entities{
			searchEntities["wordpress"].entity,
			searchEntities["wordpress-simple"].entity,
			searchEntities["squid-forwardproxy"].entity,
			searchEntities["mysql"].entity,
			searchEntities["cloud-controller-worker-v2"].entity,
			searchEntities["varnish"].entity,
		},
	}, {
		about:     "downloads descending",
		sortQuery: "-downloads",
		results: Entities{
			searchEntities["varnish"].entity,
			searchEntities["cloud-controller-worker-v2"].entity,
			searchEntities["mysql"].entity,
			searchEntities["squid-forwardproxy"].entity,
			searchEntities["wordpress-simple"].entity,
			searchEntities["wordpress"].entity,
		},
	}}
	for i, test := range tests {
		c.Logf("test %d. %s", i, test.about)
		var sp SearchParams
		err := sp.ParseSortFields(test.sortQuery)
		c.Assert(err, gc.Equals, nil)
		res, err := s.store.Search(sp)
		c.Assert(err, gc.Equals, nil)
		c.Assert(Entities(res.Results), jc.DeepEquals, test.results)
		c.Assert(res.Total, gc.Equals, len(test.results))
	}
}

func (s *StoreSearchSuite) TestBoosting(c *gc.C) {
	s.store.ES.Database.RefreshIndex(s.TestIndex)
	var sp SearchParams
	res, err := s.store.Search(sp)
	c.Assert(err, gc.Equals, nil)
	c.Assert(Entities(res.Results), jc.DeepEquals, Entities{
		searchEntities["wordpress-simple"].entity,
		searchEntities["mysql"].entity,
		searchEntities["wordpress"].entity,
		searchEntities["squid-forwardproxy"].entity,
		searchEntities["varnish"].entity,
		searchEntities["cloud-controller-worker-v2"].entity,
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
	charmArchive := storetesting.NewCharm(storetesting.MetaWithSupportedSeries(nil, "trusty", "xenial", "utopic", "vivid", "wily", "yakkety"))
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
		series: "xenial",
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
				"name":   {"juju-gui"},
				"series": {test.series},
			},
		})
		c.Assert(err, gc.Equals, nil)
		if test.notFound {
			c.Assert(res.Results, gc.HasLen, 0)
			continue
		}
		c.Assert(res.Results, gc.HasLen, 1)
		c.Assert(res.Results[0].URL.String(), gc.Equals, url.String())
	}
}

func (s *StoreSearchSuite) TestMultiSeriesCharmSortsSeriesCorrectly(c *gc.C) {
	charmArchive := storetesting.NewCharm(storetesting.MetaWithSupportedSeries(nil, "trusty", "xenial", "utopic", "vivid", "wily", "yakkety"))
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
	var sp SearchParams
	sp.ParseSortFields("-series", "owner")
	res, err := s.store.Search(sp)
	c.Assert(err, gc.Equals, nil)
	c.Assert(Entities(res.Results), jc.DeepEquals, Entities{
		newEntity("cs:~charmers/yakkety/squid-forwardproxy-3", 3),
		newEntity("cs:~charmers/juju-gui-25", -1, "trusty", "xenial", "utopic", "vivid", "wily", "yakkety"),
		newEntity("cs:~foo/xenial/varnish-1", -1),
		newEntity("cs:~openstack-charmers/xenial/mysql-7", 7),
		searchEntities["cloud-controller-worker-v2"].entity,
		newEntity("cs:~charmers/precise/wordpress-23", 23),
		newEntity("cs:~charmers/bundle/wordpress-simple-4", 4),
	})
}

func (s *StoreSearchSuite) TestOnlyIndexStableCharms(c *gc.C) {
	ch := storetesting.NewCharm(&charm.Meta{
		Name: "test",
	})
	id := router.MustNewResolvedURL("~test/xenial/test-0", -1)
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
	c.Assert(err, gc.ErrorMatches, "elasticsearch document not found")

	err = s.store.Publish(id, nil, params.EdgeChannel)
	c.Assert(err, gc.Equals, nil)
	err = s.store.UpdateSearch(id)
	c.Assert(err, gc.Equals, nil)
	err = s.store.ES.GetDocument(s.TestIndex, typeName, s.store.ES.getID(&id.URL), &actual)
	c.Assert(err, gc.ErrorMatches, "elasticsearch document not found")

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
		Series:       []string{"xenial"},
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
