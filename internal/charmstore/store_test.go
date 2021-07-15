// Copyright 2014 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package charmstore

import (
	"archive/zip"
	"bytes"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/juju/charmrepo/v6/csclient/params"
	jc "github.com/juju/testing/checkers"
	gc "gopkg.in/check.v1"
	"gopkg.in/errgo.v1"
	"gopkg.in/mgo.v2/bson"
	"gopkg.in/natefinch/lumberjack.v2"

	"gopkg.in/juju/charmstore.v5/audit"
	"gopkg.in/juju/charmstore.v5/elasticsearch"
	"gopkg.in/juju/charmstore.v5/internal/blobstore"
	"gopkg.in/juju/charmstore.v5/internal/charm"
	"gopkg.in/juju/charmstore.v5/internal/mongodoc"
	"gopkg.in/juju/charmstore.v5/internal/router"
	"gopkg.in/juju/charmstore.v5/internal/storetesting"
)

type StoreSuite struct {
	commonSuite
}

var _ = gc.Suite(&StoreSuite{})

var urlFindingTests = []struct {
	inStore []string
	expand  string
	expect  []string
}{{
	inStore: []string{"23 cs:~charmers/" + storetesting.SearchSeries[0] + "/wordpress-23"},
	expand:  "wordpress",
	expect:  []string{"23 cs:~charmers/" + storetesting.SearchSeries[0] + "/wordpress-23"},
}, {
	inStore: []string{"23 cs:~charmers/" + storetesting.SearchSeries[0] + "/wordpress-23", "24 cs:~charmers/" + storetesting.SearchSeries[0] + "/wordpress-24", "25 cs:~charmers/" + storetesting.SearchSeries[0] + "/wordpress-25"},
	expand:  "wordpress",
	expect:  []string{"23 cs:~charmers/" + storetesting.SearchSeries[0] + "/wordpress-23", "24 cs:~charmers/" + storetesting.SearchSeries[0] + "/wordpress-24", "25 cs:~charmers/" + storetesting.SearchSeries[0] + "/wordpress-25"},
}, {
	inStore: []string{"23 cs:~charmers/" + storetesting.SearchSeries[0] + "/wordpress-23", "24 cs:~charmers/" + storetesting.SearchSeries[0] + "/wordpress-24", "25 cs:~charmers/" + storetesting.SearchSeries[0] + "/wordpress-25"},
	expand:  "~charmers/" + storetesting.SearchSeries[0] + "/wordpress-24",
	expect:  []string{"24 cs:~charmers/" + storetesting.SearchSeries[0] + "/wordpress-24"},
}, {
	inStore: []string{"23 cs:~charmers/" + storetesting.SearchSeries[0] + "/wordpress-23", "24 cs:~charmers/" + storetesting.SearchSeries[0] + "/wordpress-24", "25 cs:~charmers/" + storetesting.SearchSeries[0] + "/wordpress-25"},
	expand:  "~charmers/" + storetesting.SearchSeries[0] + "/wordpress-25",
	expect:  []string{"25 cs:~charmers/" + storetesting.SearchSeries[0] + "/wordpress-25"},
}, {
	inStore: []string{"23 cs:~charmers/" + storetesting.SearchSeries[0] + "/wordpress-23", "24 cs:~charmers/" + storetesting.SearchSeries[1] + "/wordpress-24", "25 cs:~charmers/" + storetesting.SearchSeries[0] + "/wordpress-25"},
	expand:  storetesting.SearchSeries[0] + "/wordpress",
	expect:  []string{"23 cs:~charmers/" + storetesting.SearchSeries[0] + "/wordpress-23", "25 cs:~charmers/" + storetesting.SearchSeries[0] + "/wordpress-25"},
}, {
	inStore: []string{"23 cs:~charmers/" + storetesting.SearchSeries[0] + "/wordpress-23", "24 cs:~charmers/" + storetesting.SearchSeries[1] + "/wordpress-24", "434 cs:~charmers/foo/varnish-434"},
	expand:  "wordpress",
	expect:  []string{"24 cs:~charmers/" + storetesting.SearchSeries[1] + "/wordpress-24", "23 cs:~charmers/" + storetesting.SearchSeries[0] + "/wordpress-23"},
}, {
	inStore: []string{"23 cs:~charmers/" + storetesting.SearchSeries[0] + "/wordpress-23", "23 cs:~charmers/" + storetesting.SearchSeries[1] + "/wordpress-23", "24 cs:~charmers/" + storetesting.SearchSeries[1] + "/wordpress-24"},
	expand:  "wordpress-23",
	expect:  []string{},
}, {
	inStore: []string{"cs:~user/" + storetesting.SearchSeries[0] + "/wordpress-23", "cs:~user/" + storetesting.SearchSeries[1] + "/wordpress-23"},
	expand:  "~user/" + storetesting.SearchSeries[0] + "/wordpress",
	expect:  []string{"cs:~user/" + storetesting.SearchSeries[0] + "/wordpress-23"},
}, {
	inStore: []string{"cs:~user/" + storetesting.SearchSeries[0] + "/wordpress-23", "cs:~user/" + storetesting.SearchSeries[1] + "/wordpress-23"},
	expand:  "~user/wordpress",
	expect:  []string{"cs:~user/" + storetesting.SearchSeries[1] + "/wordpress-23", "cs:~user/" + storetesting.SearchSeries[0] + "/wordpress-23"},
}, {
	inStore: []string{"23 cs:~charmers/" + storetesting.SearchSeries[0] + "/wordpress-23", "24 cs:~charmers/" + storetesting.SearchSeries[1] + "/wordpress-24", "434 cs:~charmers/foo/varnish-434"},
	expand:  storetesting.SearchSeries[0] + "/wordpress-23",
	expect:  []string{"23 cs:~charmers/" + storetesting.SearchSeries[0] + "/wordpress-23"},
}, {
	inStore: []string{"23 cs:~charmers/" + storetesting.SearchSeries[0] + "/wordpress-23", "24 cs:~charmers/" + storetesting.SearchSeries[1] + "/wordpress-24", "434 cs:~charmers/foo/varnish-434"},
	expand:  "arble",
	expect:  []string{},
}, {
	inStore: []string{"23 cs:~charmers/multi-series-23", "24 cs:~charmers/multi-series-24"},
	expand:  "multi-series",
	expect:  []string{"23 cs:~charmers/multi-series-23", "24 cs:~charmers/multi-series-24"},
}, {
	inStore: []string{"23 cs:~charmers/multi-series-23", "24 cs:~charmers/multi-series-24"},
	expand:  storetesting.SearchSeries[1] + "/multi-series",
	expect:  []string{"23 cs:~charmers/multi-series-23", "24 cs:~charmers/multi-series-24"},
}, {
	inStore: []string{"23 cs:~charmers/multi-series-23", "24 cs:~charmers/multi-series-24"},
	expand:  "multi-series-24",
	expect:  []string{"24 cs:~charmers/multi-series-24"},
}, {
	inStore: []string{"23 cs:~charmers/multi-series-23", "24 cs:~charmers/multi-series-24"},
	expand:  storetesting.SearchSeries[1] + "/multi-series-24",
	expect:  []string{"24 cs:~charmers/multi-series-24"},
}, {
	inStore: []string{"1 cs:~charmers/multi-series-23", "2 cs:~charmers/multi-series-24"},
	expand:  storetesting.SearchSeries[1] + "/multi-series-1",
	expect:  []string{"1 cs:~charmers/multi-series-23"},
}, {
	inStore: []string{"1 cs:~charmers/multi-series-23", "2 cs:~charmers/multi-series-24"},
	expand:  "multi-series-23",
	expect:  []string{},
}, {
	inStore: []string{"1 cs:~charmers/multi-series-23", "2 cs:~charmers/multi-series-24"},
	expand:  "cs:~charmers/" + storetesting.SearchSeries[1] + "/multi-series-23",
	expect:  []string{"1 cs:~charmers/multi-series-23"},
}, {
	inStore: []string{},
	expand:  storetesting.SearchSeries[0] + "/wordpress-23",
	expect:  []string{},
}}

func (s *StoreSuite) testURLFinding(c *gc.C, check func(store *Store, expand *charm.URL, expect []*router.ResolvedURL)) {
	store := s.newStore(c, false)
	defer store.Close()
	for i, test := range urlFindingTests {
		c.Logf("test %d: %q from %q", i, test.expand, test.inStore)
		_, err := store.DB.Entities().RemoveAll(nil)
		c.Assert(err, gc.Equals, nil)
		urls := MustParseResolvedURLs(test.inStore)
		for _, url := range urls {
			var m *charm.Meta
			if url.URL.Series == "" {
				m = storetesting.MetaWithSupportedSeries(m, storetesting.SearchSeries...)
			}
			err := store.AddCharmWithArchive(url, storetesting.NewCharm(m))
			c.Assert(err, gc.Equals, nil)
		}
		check(store, charm.MustParseURL(test.expand), MustParseResolvedURLs(test.expect))
	}
}

func (s *StoreSuite) TestRequestStore(c *gc.C) {
	config := ServerParams{
		HTTPRequestWaitDuration: time.Millisecond,
		MaxMgoSessions:          1,
	}
	p, err := NewPool(s.Session.DB("juju_test"), nil, nil, config)
	c.Assert(err, gc.Equals, nil)
	defer p.Close()

	// Instances within the limit can be acquired
	// instantly without error.
	store, err := p.RequestStore()
	c.Assert(err, gc.Equals, nil)
	store.Close()

	// Check that when we get another instance,
	// we reuse the original.
	store1, err := p.RequestStore()
	c.Assert(err, gc.Equals, nil)
	defer store1.Close()
	c.Assert(store1, gc.Equals, store)

	// If we try to exceed the limit, we'll wait for a while,
	// then return an error.
	t0 := time.Now()
	store2, err := p.RequestStore()
	c.Assert(err, gc.ErrorMatches, "too many mongo sessions in use")
	c.Assert(errgo.Cause(err), gc.Equals, ErrTooManySessions)
	c.Assert(store2, gc.IsNil)
	if d := time.Since(t0); d < config.HTTPRequestWaitDuration {
		c.Errorf("got wait of %v; want at least %v", d, config.HTTPRequestWaitDuration)
	}
}

func (s *StoreSuite) TestRequestStoreSatisfiedWithinTimeout(c *gc.C) {
	config := ServerParams{
		HTTPRequestWaitDuration: 5 * time.Second,
		MaxMgoSessions:          1,
	}
	p, err := NewPool(s.Session.DB("juju_test"), nil, nil, config)
	c.Assert(err, gc.Equals, nil)
	defer p.Close()
	store, err := p.RequestStore()
	c.Assert(err, gc.Equals, nil)

	// Start a goroutine that will close the Store after a short period.
	go func() {
		time.Sleep(time.Millisecond)
		store.Close()
	}()
	store1, err := p.RequestStore()
	c.Assert(err, gc.Equals, nil)
	c.Assert(store1, gc.Equals, store)
	store1.Close()
}

func (s *StoreSuite) TestRequestStoreLimitCanBeExceeded(c *gc.C) {
	config := ServerParams{
		HTTPRequestWaitDuration: 5 * time.Second,
		MaxMgoSessions:          1,
	}
	p, err := NewPool(s.Session.DB("juju_test"), nil, nil, config)
	c.Assert(err, gc.Equals, nil)
	defer p.Close()
	store, err := p.RequestStore()
	c.Assert(err, gc.Equals, nil)
	defer store.Close()

	store1 := store.Copy()
	defer store1.Close()
	c.Assert(store1.Pool(), gc.Equals, store.Pool())

	store2 := p.Store()
	defer store2.Close()
	c.Assert(store2.Pool(), gc.Equals, store.Pool())
}

func (s *StoreSuite) TestRequestStoreFailsWhenPoolIsClosed(c *gc.C) {
	config := ServerParams{
		HTTPRequestWaitDuration: 5 * time.Second,
		MaxMgoSessions:          1,
	}
	p, err := NewPool(s.Session.DB("juju_test"), nil, nil, config)
	c.Assert(err, gc.Equals, nil)
	p.Close()
	store, err := p.RequestStore()
	c.Assert(err, gc.ErrorMatches, "charm store has been closed")
	c.Assert(store, gc.IsNil)
}

func (s *StoreSuite) TestRequestStoreLimitMaintained(c *gc.C) {
	config := ServerParams{
		HTTPRequestWaitDuration: time.Millisecond,
		MaxMgoSessions:          1,
	}
	p, err := NewPool(s.Session.DB("juju_test"), nil, nil, config)
	c.Assert(err, gc.Equals, nil)
	defer p.Close()

	// Acquire an instance.
	store, err := p.RequestStore()
	c.Assert(err, gc.Equals, nil)
	defer store.Close()

	// Acquire another instance, exceeding the limit,
	// and put it back.
	store1 := p.Store()
	c.Assert(err, gc.Equals, nil)
	store1.Close()

	// We should still be unable to acquire another
	// store for a request because we're still
	// at the request limit.
	_, err = p.RequestStore()
	c.Assert(errgo.Cause(err), gc.Equals, ErrTooManySessions)
}

func (s *StoreSuite) TestPoolDoubleClose(c *gc.C) {
	p, err := NewPool(s.Session.DB("juju_test"), nil, nil, ServerParams{})
	c.Assert(err, gc.Equals, nil)
	p.Close()
	p.Close()

	// Close a third time to ensure that the lock has properly
	// been released.
	p.Close()
}

func (s *StoreSuite) TestFindEntities(c *gc.C) {
	s.testURLFinding(c, func(store *Store, expand *charm.URL, expect []*router.ResolvedURL) {
		// Check FindEntities works when just retrieving the id and promulgated id.
		gotEntities, err := store.FindEntities(expand, FieldSelector("_id", "promulgated-url"))
		c.Assert(err, gc.Equals, nil)
		if expand.User == "" {
			sort.Sort(entitiesByPromulgatedURL(gotEntities))
		} else {
			sort.Sort(entitiesByURL(gotEntities))
		}
		c.Assert(gotEntities, gc.HasLen, len(expect))
		for i, url := range expect {
			c.Check(gotEntities[i], jc.DeepEquals, &mongodoc.Entity{
				URL:            &url.URL,
				PromulgatedURL: url.PromulgatedURL(),
			}, gc.Commentf("index %d", i))
		}

		// check FindEntities works when retrieving all fields.
		gotEntities, err = store.FindEntities(expand, nil)
		c.Assert(err, gc.Equals, nil)
		if expand.User == "" {
			sort.Sort(entitiesByPromulgatedURL(gotEntities))
		} else {
			sort.Sort(entitiesByURL(gotEntities))
		}
		c.Assert(gotEntities, gc.HasLen, len(expect))
		for i, url := range expect {
			var entity mongodoc.Entity
			err := store.DB.Entities().FindId(&url.URL).One(&entity)
			c.Assert(err, gc.Equals, nil)
			c.Assert(gotEntities[i], jc.DeepEquals, &entity)
		}
	})
}

func (s *StoreSuite) TestFindEntity(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()

	rurl := MustParseResolvedURL("cs:~charmers/" + storetesting.SearchSeries[0] + "/wordpress-5")
	err := store.AddCharmWithArchive(rurl, storetesting.Charms.CharmDir("wordpress"))
	c.Assert(err, gc.Equals, nil)

	entity0, err := store.FindEntity(rurl, nil)
	c.Assert(err, gc.Equals, nil)
	c.Assert(entity0, gc.NotNil)
	c.Assert(entity0.Size, gc.Not(gc.Equals), 0)

	// Check that the field selector works.
	entity2, err := store.FindEntity(rurl, FieldSelector("blobhash"))
	c.Assert(err, gc.Equals, nil)
	c.Assert(entity2.BlobHash, gc.Equals, entity0.BlobHash)
	c.Assert(entity2.Size, gc.Equals, int64(0))

	rurl.URL.Name = "another"
	entity3, err := store.FindEntity(rurl, nil)
	c.Assert(err, gc.ErrorMatches, "entity not found")
	c.Assert(errgo.Cause(err), gc.Equals, params.ErrNotFound)
	c.Assert(entity3, gc.IsNil)
}

var findBaseEntityTests = []struct {
	about  string
	stored []string
	url    string
	fields []string
	expect *mongodoc.BaseEntity
}{{
	about:  "entity found, base url, all fields",
	stored: []string{"42 cs:~charmers/utopic/mysql-42"},
	url:    "mysql",
	expect: storetesting.NormalizeBaseEntity(&mongodoc.BaseEntity{
		URL:         charm.MustParseURL("~charmers/mysql"),
		User:        "charmers",
		Name:        "mysql",
		Promulgated: true,
		ChannelACLs: map[params.Channel]mongodoc.ACL{
			params.UnpublishedChannel: {
				Read:  []string{"charmers"},
				Write: []string{"charmers"},
			},
			params.EdgeChannel: {
				Read:  []string{"charmers"},
				Write: []string{"charmers"},
			},
			params.BetaChannel: {
				Read:  []string{"charmers"},
				Write: []string{"charmers"},
			},
			params.CandidateChannel: {
				Read:  []string{"charmers"},
				Write: []string{"charmers"},
			},
			params.StableChannel: {
				Read:  []string{"charmers"},
				Write: []string{"charmers"},
			},
		},
	}),
}, {
	about:  "entity found, fully qualified url, few fields",
	stored: []string{"42 cs:~charmers/utopic/mysql-42", "~who/" + storetesting.SearchSeries[0] + "/mysql-47"},
	url:    "~who/" + storetesting.SearchSeries[0] + "/mysql-0",
	fields: []string{"user"},
	expect: &mongodoc.BaseEntity{
		URL:  charm.MustParseURL("~who/mysql"),
		User: "who",
	},
}, {
	about:  "entity found, partial url, only the ACLs",
	stored: []string{"42 cs:~charmers/utopic/mysql-42", "~who/" + storetesting.SearchSeries[1] + "/mysql-47"},
	url:    "~who/mysql-42",
	fields: []string{"channelacls"},
	expect: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/mysql"),
		ChannelACLs: map[params.Channel]mongodoc.ACL{
			params.UnpublishedChannel: {
				Read:  []string{"who"},
				Write: []string{"who"},
			},
			params.EdgeChannel: {
				Read:  []string{"who"},
				Write: []string{"who"},
			},
			params.BetaChannel: {
				Read:  []string{"who"},
				Write: []string{"who"},
			},
			params.CandidateChannel: {
				Read:  []string{"who"},
				Write: []string{"who"},
			},
			params.StableChannel: {
				Read:  []string{"who"},
				Write: []string{"who"},
			},
		},
	},
}, {
	about:  "entity not found, charm name",
	stored: []string{"42 cs:~charmers/utopic/mysql-42", "~who/" + storetesting.SearchSeries[1] + "/mysql-47"},
	url:    "rails",
}, {
	about:  "entity not found, user",
	stored: []string{"42 cs:~charmers/utopic/mysql-42", "~who/" + storetesting.SearchSeries[1] + "/mysql-47"},
	url:    "~dalek/mysql",
	fields: []string{"channelacls"},
}}

func (s *StoreSuite) TestFindBaseEntity(c *gc.C) {
	ch := storetesting.Charms.CharmDir("wordpress")
	store := s.newStore(c, false)
	defer store.Close()
	for i, test := range findBaseEntityTests {
		c.Logf("test %d: %s", i, test.about)

		// Add initial charms to the store.
		for _, url := range MustParseResolvedURLs(test.stored) {
			err := store.AddCharmWithArchive(url, ch)
			c.Assert(err, gc.Equals, nil)
		}

		// Find the entity.
		id := charm.MustParseURL(test.url)
		baseEntity, err := store.FindBaseEntity(id, FieldSelector(test.fields...))
		if test.expect == nil {
			// We don't expect the entity to be found.
			c.Assert(errgo.Cause(err), gc.Equals, params.ErrNotFound)
			c.Assert(baseEntity, gc.IsNil)
		} else {
			c.Assert(err, gc.Equals, nil)
			c.Assert(storetesting.NormalizeBaseEntity(baseEntity), jc.DeepEquals, storetesting.NormalizeBaseEntity(test.expect))
		}

		// Remove all the entities from the store.
		_, err = store.DB.Entities().RemoveAll(nil)
		c.Assert(err, gc.Equals, nil)
		_, err = store.DB.BaseEntities().RemoveAll(nil)
		c.Assert(err, gc.Equals, nil)
	}
}

func (s *StoreSuite) TestAddCharmsWithTheSameBaseEntity(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()

	// Add a charm to the database.
	ch := storetesting.Charms.CharmDir("wordpress")
	url := router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/wordpress-12", 12)
	err := store.AddCharmWithArchive(url, ch)
	c.Assert(err, gc.Equals, nil)

	// Add a second charm to the database, sharing the same base URL.
	err = store.AddCharmWithArchive(router.MustNewResolvedURL("~charmers/utopic/wordpress-13", -1), ch)
	c.Assert(err, gc.Equals, nil)

	// Ensure a single base entity has been created.
	num, err := store.DB.BaseEntities().Count()
	c.Assert(err, gc.Equals, nil)
	c.Assert(num, gc.Equals, 1)
}

type entitiesByURL []*mongodoc.Entity

func (s entitiesByURL) Len() int      { return len(s) }
func (s entitiesByURL) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s entitiesByURL) Less(i, j int) bool {
	return s[i].URL.String() < s[j].URL.String()
}

type entitiesByPromulgatedURL []*mongodoc.Entity

func (s entitiesByPromulgatedURL) Len() int      { return len(s) }
func (s entitiesByPromulgatedURL) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s entitiesByPromulgatedURL) Less(i, j int) bool {
	return s[i].PromulgatedURL.String() < s[j].PromulgatedURL.String()
}

var bundleUnitCountTests = []struct {
	about       string
	data        *charm.BundleData
	expectUnits int
}{{
	about: "no units",
	data: &charm.BundleData{
		Applications: map[string]*charm.ApplicationSpec{
			"wordpress": {
				Charm:    "cs:utopic/wordpress-0",
				NumUnits: 0,
			},
			"mysql": {
				Charm:    "cs:" + storetesting.SearchSeries[1] + "/mysql-0",
				NumUnits: 0,
			},
		},
	},
}, {
	about: "a single unit",
	data: &charm.BundleData{
		Applications: map[string]*charm.ApplicationSpec{
			"wordpress": {
				Charm:    "cs:" + storetesting.SearchSeries[1] + "/wordpress-42",
				NumUnits: 1,
			},
			"mysql": {
				Charm:    "cs:" + storetesting.SearchSeries[1] + "/mysql-47",
				NumUnits: 0,
			},
		},
	},
	expectUnits: 1,
}, {
	about: "multiple units",
	data: &charm.BundleData{
		Applications: map[string]*charm.ApplicationSpec{
			"wordpress": {
				Charm:    "cs:utopic/wordpress-1",
				NumUnits: 1,
			},
			"mysql": {
				Charm:    "cs:utopic/mysql-2",
				NumUnits: 2,
			},
			"riak": {
				Charm:    "cs:utopic/riak-3",
				NumUnits: 5,
			},
		},
	},
	expectUnits: 8,
}}

func (s *StoreSuite) TestBundleUnitCount(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()
	entities := store.DB.Entities()
	for i, test := range bundleUnitCountTests {
		c.Logf("test %d: %s", i, test.about)
		url := router.MustNewResolvedURL("cs:~charmers/bundle/wordpress-simple-0", -1)
		url.URL.Revision = i
		url.PromulgatedRevision = i

		// Add the bundle used for this test.
		b := storetesting.NewBundle(test.data)
		s.addRequiredCharms(c, b)
		err := store.AddBundleWithArchive(url, b)
		c.Assert(err, gc.Equals, nil)

		// Retrieve the bundle from the database.
		var doc mongodoc.Entity
		err = entities.FindId(&url.URL).One(&doc)
		c.Assert(err, gc.Equals, nil)

		c.Assert(*doc.BundleUnitCount, gc.Equals, test.expectUnits)
	}
}

var bundleMachineCountTests = []struct {
	about          string
	data           *charm.BundleData
	expectMachines int
}{{
	about: "no machines",
	data: &charm.BundleData{
		Applications: map[string]*charm.ApplicationSpec{
			"mysql": {
				Charm:    "cs:utopic/mysql-0",
				NumUnits: 0,
			},
			"wordpress": {
				Charm:    "cs:" + storetesting.SearchSeries[1] + "/wordpress-0",
				NumUnits: 0,
			},
		},
	},
}, {
	about: "a single machine (no placement)",
	data: &charm.BundleData{
		Applications: map[string]*charm.ApplicationSpec{
			"mysql": {
				Charm:    "cs:" + storetesting.SearchSeries[1] + "/mysql-42",
				NumUnits: 1,
			},
			"wordpress": {
				Charm:    "cs:" + storetesting.SearchSeries[1] + "/wordpress-47",
				NumUnits: 0,
			},
		},
	},
	expectMachines: 1,
}, {
	about: "a single machine (machine placement)",
	data: &charm.BundleData{
		Applications: map[string]*charm.ApplicationSpec{
			"mysql": {
				Charm:    "cs:" + storetesting.SearchSeries[1] + "/mysql-42",
				NumUnits: 1,
				To:       []string{"1"},
			},
		},
		Machines: map[string]*charm.MachineSpec{
			"1": nil,
		},
	},
	expectMachines: 1,
}, {
	about: "a single machine (hulk smash)",
	data: &charm.BundleData{
		Applications: map[string]*charm.ApplicationSpec{
			"mysql": {
				Charm:    "cs:" + storetesting.SearchSeries[1] + "/mysql-42",
				NumUnits: 1,
				To:       []string{"1"},
			},
			"wordpress": {
				Charm:    "cs:" + storetesting.SearchSeries[1] + "/wordpress-47",
				NumUnits: 1,
				To:       []string{"1"},
			},
		},
		Machines: map[string]*charm.MachineSpec{
			"1": nil,
		},
	},
	expectMachines: 1,
}, {
	about: "a single machine (co-location)",
	data: &charm.BundleData{
		Applications: map[string]*charm.ApplicationSpec{
			"mysql": {
				Charm:    "cs:" + storetesting.SearchSeries[1] + "/mysql-42",
				NumUnits: 1,
			},
			"wordpress": {
				Charm:    "cs:" + storetesting.SearchSeries[1] + "/wordpress-47",
				NumUnits: 1,
				To:       []string{"mysql/0"},
			},
		},
	},
	expectMachines: 1,
}, {
	about: "a single machine (containerization)",
	data: &charm.BundleData{
		Applications: map[string]*charm.ApplicationSpec{
			"mysql": {
				Charm:    "cs:" + storetesting.SearchSeries[1] + "/mysql-42",
				NumUnits: 1,
				To:       []string{"1"},
			},
			"wordpress": {
				Charm:    "cs:" + storetesting.SearchSeries[1] + "/wordpress-47",
				NumUnits: 1,
				To:       []string{"lxc:1"},
			},
			"riak": {
				Charm:    "cs:utopic/riak-3",
				NumUnits: 2,
				To:       []string{"kvm:1"},
			},
		},
		Machines: map[string]*charm.MachineSpec{
			"1": nil,
		},
	},
	expectMachines: 1,
}, {
	about: "multiple machines (no placement)",
	data: &charm.BundleData{
		Applications: map[string]*charm.ApplicationSpec{
			"mysql": {
				Charm:    "cs:utopic/mysql-1",
				NumUnits: 1,
			},
			"wordpress": {
				Charm:    "cs:utopic/wordpress-2",
				NumUnits: 2,
			},
			"riak": {
				Charm:    "cs:utopic/riak-3",
				NumUnits: 5,
			},
		},
	},
	expectMachines: 1 + 2 + 5,
}, {
	about: "multiple machines (machine placement)",
	data: &charm.BundleData{
		Applications: map[string]*charm.ApplicationSpec{
			"mysql": {
				Charm:    "cs:utopic/mysql-1",
				NumUnits: 2,
				To:       []string{"1", "3"},
			},
			"wordpress": {
				Charm:    "cs:utopic/wordpress-2",
				NumUnits: 1,
				To:       []string{"2"},
			},
		},
		Machines: map[string]*charm.MachineSpec{
			"1": nil, "2": nil, "3": nil,
		},
	},
	expectMachines: 2 + 1,
}, {
	about: "multiple machines (hulk smash)",
	data: &charm.BundleData{
		Applications: map[string]*charm.ApplicationSpec{
			"mysql": {
				Charm:    "cs:" + storetesting.SearchSeries[1] + "/mysql-42",
				NumUnits: 1,
				To:       []string{"1"},
			},
			"wordpress": {
				Charm:    "cs:" + storetesting.SearchSeries[1] + "/wordpress-47",
				NumUnits: 1,
				To:       []string{"2"},
			},
			"riak": {
				Charm:    "cs:utopic/riak-3",
				NumUnits: 2,
				To:       []string{"1", "2"},
			},
		},
		Machines: map[string]*charm.MachineSpec{
			"1": nil, "2": nil,
		},
	},
	expectMachines: 1 + 1 + 0,
}, {
	about: "multiple machines (co-location)",
	data: &charm.BundleData{
		Applications: map[string]*charm.ApplicationSpec{
			"mysql": {
				Charm:    "cs:" + storetesting.SearchSeries[1] + "/mysql-42",
				NumUnits: 2,
			},
			"wordpress": {
				Charm:    "cs:" + storetesting.SearchSeries[1] + "/wordpress-47",
				NumUnits: 3,
				To:       []string{"mysql/0", "mysql/1", "new"},
			},
		},
	},
	expectMachines: 2 + 1,
}, {
	about: "multiple machines (containerization)",
	data: &charm.BundleData{
		Applications: map[string]*charm.ApplicationSpec{
			"mysql": {
				Charm:    "cs:" + storetesting.SearchSeries[1] + "/mysql-42",
				NumUnits: 2,
				To:       []string{"1", "2"},
			},
			"wordpress": {
				Charm:    "cs:" + storetesting.SearchSeries[1] + "/wordpress-47",
				NumUnits: 4,
				To:       []string{"lxc:1", "lxc:2", "lxc:3", "lxc:3"},
			},
			"riak": {
				Charm:    "cs:utopic/riak-3",
				NumUnits: 1,
				To:       []string{"kvm:2"},
			},
		},
		Machines: map[string]*charm.MachineSpec{
			"1": nil, "2": nil, "3": nil,
		},
	},
	expectMachines: 2 + 1 + 0,
}, {
	about: "multiple machines (partial placement in a container)",
	data: &charm.BundleData{
		Applications: map[string]*charm.ApplicationSpec{
			"mysql": {
				Charm:    "cs:" + storetesting.SearchSeries[1] + "/mysql-42",
				NumUnits: 1,
				To:       []string{"1"},
			},
			"wordpress": {
				Charm:    "cs:" + storetesting.SearchSeries[1] + "/wordpress-47",
				NumUnits: 10,
				To:       []string{"lxc:1", "lxc:2"},
			},
		},
		Machines: map[string]*charm.MachineSpec{
			"1": nil, "2": nil,
		},
	},
	expectMachines: 1 + 1,
}, {
	about: "multiple machines (partial placement in a new machine)",
	data: &charm.BundleData{
		Applications: map[string]*charm.ApplicationSpec{
			"mysql": {
				Charm:    "cs:" + storetesting.SearchSeries[1] + "/mysql-42",
				NumUnits: 1,
				To:       []string{"1"},
			},
			"wordpress": {
				Charm:    "cs:" + storetesting.SearchSeries[1] + "/wordpress-47",
				NumUnits: 10,
				To:       []string{"lxc:1", "1", "new"},
			},
		},
		Machines: map[string]*charm.MachineSpec{
			"1": nil,
		},
	},
	expectMachines: 1 + 8,
}, {
	about: "multiple machines (partial placement with new machines)",
	data: &charm.BundleData{
		Applications: map[string]*charm.ApplicationSpec{
			"mysql": {
				Charm:    "cs:" + storetesting.SearchSeries[1] + "/mysql-42",
				NumUnits: 3,
			},
			"wordpress": {
				Charm:    "cs:" + storetesting.SearchSeries[1] + "/wordpress-47",
				NumUnits: 6,
				To:       []string{"new", "1", "lxc:1", "new"},
			},
			"riak": {
				Charm:    "cs:utopic/riak-3",
				NumUnits: 10,
				To:       []string{"kvm:2", "lxc:mysql/1", "new", "new", "kvm:2"},
			},
		},
		Machines: map[string]*charm.MachineSpec{
			"1": nil, "2": nil,
		},
	},
	expectMachines: 3 + 5 + 3,
}, {
	about: "placement into container on new machine",
	data: &charm.BundleData{
		Applications: map[string]*charm.ApplicationSpec{
			"wordpress": {
				Charm:    "cs:" + storetesting.SearchSeries[1] + "/wordpress-47",
				NumUnits: 6,
				To:       []string{"lxc:new", "1", "lxc:1", "kvm:new"},
			},
		},
		Machines: map[string]*charm.MachineSpec{
			"1": nil,
		},
	},
	expectMachines: 5,
}}

func (s *StoreSuite) TestBundleMachineCount(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()
	entities := store.DB.Entities()
	for i, test := range bundleMachineCountTests {
		c.Logf("test %d: %s", i, test.about)
		url := router.MustNewResolvedURL("cs:~charmers/bundle/testbundle-0", -1)
		url.URL.Revision = i
		url.PromulgatedRevision = i
		err := test.data.Verify(nil, nil, nil)
		c.Assert(err, gc.Equals, nil)
		// Add the bundle used for this test.
		b := storetesting.NewBundle(test.data)
		s.addRequiredCharms(c, b)
		err = store.AddBundleWithArchive(url, b)
		c.Assert(err, gc.Equals, nil)

		// Retrieve the bundle from the database.
		var doc mongodoc.Entity
		err = entities.FindId(&url.URL).One(&doc)
		c.Assert(err, gc.Equals, nil)

		c.Assert(*doc.BundleMachineCount, gc.Equals, test.expectMachines)
	}
}

func (s *StoreSuite) TestOpenBlob(c *gc.C) {
	charmArchive := storetesting.Charms.CharmArchive(c.MkDir(), "wordpress")
	store := s.newStore(c, false)
	defer store.Close()
	url := router.MustNewResolvedURL("cs:~charmers/"+storetesting.SearchSeries[0]+"/wordpress-23", 23)
	err := store.AddCharmWithArchive(url, charmArchive)
	c.Assert(err, gc.Equals, nil)

	f, err := os.Open(charmArchive.Path)
	c.Assert(err, gc.Equals, nil)
	defer f.Close()
	expectHash := hashOfReader(f)

	blob, err := store.OpenBlob(url)
	c.Assert(err, gc.Equals, nil)
	defer blob.Close()

	c.Assert(hashOfReader(blob), gc.Equals, expectHash)
	c.Assert(blob.Hash, gc.Equals, expectHash)

	info, err := f.Stat()
	c.Assert(err, gc.Equals, nil)
	c.Assert(blob.Size, gc.Equals, info.Size())
}

func (s *StoreSuite) TestOpenBlobPreV5(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()
	ch := storetesting.NewCharm(storetesting.MetaWithSupportedSeries(nil, storetesting.SearchSeries[1], storetesting.SearchSeries[0]))

	url := router.MustNewResolvedURL("cs:~charmers/multi-series-23", 23)
	err := store.AddCharmWithArchive(url, ch)
	c.Assert(err, gc.Equals, nil)

	blob, err := store.OpenBlobPreV5(url)
	c.Assert(err, gc.Equals, nil)
	defer blob.Close()

	data, err := ioutil.ReadAll(blob)
	c.Assert(err, gc.Equals, nil)
	preV5Ch, err := charm.ReadCharmArchiveBytes(data)
	c.Assert(err, gc.Equals, nil)

	// Check that the hashes and sizes are consistent with the data
	// we've read.
	c.Assert(blob.Hash, gc.Equals, fmt.Sprintf("%x", sha512.Sum384(data)))
	c.Assert(blob.Size, gc.Equals, int64(len(data)))

	entity, err := store.FindEntity(url, nil)
	c.Assert(err, gc.Equals, nil)

	c.Assert(entity.PreV5BlobHash, gc.Equals, blob.Hash)
	c.Assert(entity.PreV5BlobHash256, gc.Equals, fmt.Sprintf("%x", sha256.Sum256(data)))
	c.Assert(entity.PreV5BlobSize, gc.Equals, blob.Size)

	c.Assert(preV5Ch.Meta().Series, gc.HasLen, 0)

	// Check that the series really are in the post-v5 blob.
	blob, err = store.OpenBlob(url)
	c.Assert(err, gc.Equals, nil)
	defer blob.Close()

	data, err = ioutil.ReadAll(blob)
	c.Assert(err, gc.Equals, nil)

	postV5Ch, err := charm.ReadCharmArchiveBytes(data)
	c.Assert(err, gc.Equals, nil)

	c.Assert(postV5Ch.Meta().Series, jc.DeepEquals, []string{storetesting.SearchSeries[1], storetesting.SearchSeries[0]})
}

func (s *StoreSuite) TestOpenBlobPreV5WithMultiSeriesCharmInSingleSeriesId(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()
	ch := storetesting.NewCharm(storetesting.MetaWithSupportedSeries(nil, storetesting.SearchSeries[1], storetesting.SearchSeries[0]))

	url := router.MustNewResolvedURL("cs:~charmers/"+storetesting.SearchSeries[0]+"/multi-series-23", 23)
	err := store.AddCharmWithArchive(url, ch)
	c.Assert(err, gc.Equals, nil)

	blob, err := store.OpenBlobPreV5(url)
	c.Assert(err, gc.Equals, nil)
	defer blob.Close()

	data, err := ioutil.ReadAll(blob)
	c.Assert(err, gc.Equals, nil)
	preV5Ch, err := charm.ReadCharmArchiveBytes(data)
	c.Assert(err, gc.Equals, nil)

	c.Assert(preV5Ch.Meta().Series, gc.HasLen, 0)
}

func (s *StoreSuite) TestAddLog(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()
	urls := []*charm.URL{
		charm.MustParseURL("cs:mysql"),
		charm.MustParseURL("cs:rails"),
	}
	infoData := json.RawMessage([]byte(`"info data"`))
	errorData := json.RawMessage([]byte(`"error data"`))

	// Add logs to the store.
	beforeAdding := time.Now().Add(-time.Second)
	err := store.AddLog(&infoData, mongodoc.InfoLevel, mongodoc.IngestionType, nil)
	c.Assert(err, gc.Equals, nil)
	err = store.AddLog(&errorData, mongodoc.ErrorLevel, mongodoc.IngestionType, urls)
	c.Assert(err, gc.Equals, nil)
	afterAdding := time.Now().Add(time.Second)

	// Retrieve the logs from the store.
	var docs []mongodoc.Log
	err = store.DB.Logs().Find(nil).Sort("_id").All(&docs)
	c.Assert(err, gc.Equals, nil)
	c.Assert(docs, gc.HasLen, 2)

	// The docs have been correctly added to the Mongo collection.
	infoDoc, errorDoc := docs[0], docs[1]
	c.Assert(infoDoc.Time, jc.TimeBetween(beforeAdding, afterAdding))
	c.Assert(errorDoc.Time, jc.TimeBetween(beforeAdding, afterAdding))
	infoDoc.Time = time.Time{}
	errorDoc.Time = time.Time{}
	c.Assert(infoDoc, jc.DeepEquals, mongodoc.Log{
		Data:  []byte(infoData),
		Level: mongodoc.InfoLevel,
		Type:  mongodoc.IngestionType,
		URLs:  nil,
	})
	c.Assert(errorDoc, jc.DeepEquals, mongodoc.Log{
		Data:  []byte(errorData),
		Level: mongodoc.ErrorLevel,
		Type:  mongodoc.IngestionType,
		URLs:  urls,
	})
}

func (s *StoreSuite) TestAddLogDataError(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()
	data := json.RawMessage([]byte("!"))

	// Try to add the invalid log message to the store.
	err := store.AddLog(&data, mongodoc.InfoLevel, mongodoc.IngestionType, nil)
	c.Assert(err, gc.ErrorMatches, "cannot marshal log data: json: error calling MarshalJSON .*")
}

func (s *StoreSuite) TestAddLogBaseURLs(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()

	// Add the log to the store with associated URLs.
	data := json.RawMessage([]byte(`"info data"`))
	err := store.AddLog(&data, mongodoc.WarningLevel, mongodoc.IngestionType, []*charm.URL{
		charm.MustParseURL(storetesting.SearchSeries[1] + "/mysql-42"),
		charm.MustParseURL("~who/utopic/wordpress"),
	})
	c.Assert(err, gc.Equals, nil)

	// Retrieve the log from the store.
	var doc mongodoc.Log
	err = store.DB.Logs().Find(nil).One(&doc)
	c.Assert(err, gc.Equals, nil)

	// The log includes the base URLs.
	c.Assert(doc.URLs, jc.DeepEquals, []*charm.URL{
		charm.MustParseURL(storetesting.SearchSeries[1] + "/mysql-42"),
		charm.MustParseURL("mysql"),
		charm.MustParseURL("~who/utopic/wordpress"),
		charm.MustParseURL("~who/wordpress"),
	})
}

func (s *StoreSuite) TestAddLogDuplicateURLs(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()

	// Add the log to the store with associated URLs.
	data := json.RawMessage([]byte(`"info data"`))
	err := store.AddLog(&data, mongodoc.WarningLevel, mongodoc.IngestionType, []*charm.URL{
		charm.MustParseURL(storetesting.SearchSeries[1] + "/mysql-42"),
		charm.MustParseURL("mysql"),
		charm.MustParseURL(storetesting.SearchSeries[1] + "/mysql-42"),
		charm.MustParseURL("mysql"),
	})
	c.Assert(err, gc.Equals, nil)

	// Retrieve the log from the store.
	var doc mongodoc.Log
	err = store.DB.Logs().Find(nil).One(&doc)
	c.Assert(err, gc.Equals, nil)

	// The log excludes duplicate URLs.
	c.Assert(doc.URLs, jc.DeepEquals, []*charm.URL{
		charm.MustParseURL(storetesting.SearchSeries[1] + "/mysql-42"),
		charm.MustParseURL("mysql"),
	})
}

func (s *StoreSuite) TestCollections(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()
	colls := store.DB.Collections()
	names, err := store.DB.CollectionNames()
	c.Assert(err, gc.Equals, nil)
	// Some collections don't have indexes so they are created only when used.
	createdOnUse := map[string]bool{
		"migrations": true,
	}
	// Check that all collections mentioned by Collections are actually created.
	for _, coll := range colls {
		found := false
		for _, name := range names {
			if name == coll.Name || createdOnUse[coll.Name] {
				found = true
			}
		}
		if !found {
			c.Errorf("collection %q not created", coll.Name)
		}

	}
	otherCollections := map[string]bool{
		"managedStoredResources": true,
		"entitystore.chunks":     true,
		"entitystore.blobref":    true,
		"storedResources":        true,
		"txns":                   true,
		"txns.log":               true,
		"txns.stash":             true,
	}
	// Check that all created collections are mentioned in Collections.
	for _, name := range names {
		if strings.HasPrefix(name, "system.") || strings.HasPrefix(name, "blobstore.") || otherCollections[name] {
			continue
		}
		if name == "system.indexes" || name == "managedStoredResources" || name == "entitystore.files" {
			continue
		}
		found := false
		for _, coll := range colls {
			if coll.Name == name {
				found = true
			}
		}
		if !found {
			c.Errorf("extra collection %q found", name)
		}
	}
}

func (s *StoreSuite) TestOpenCachedBlobFileWithInvalidEntity(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()

	wordpress := storetesting.Charms.CharmDir("wordpress")
	url := router.MustNewResolvedURL("cs:~charmers/"+storetesting.SearchSeries[0]+"/wordpress-23", 23)
	err := store.AddCharmWithArchive(url, wordpress)
	c.Assert(err, gc.Equals, nil)

	entity, err := store.FindEntity(url, FieldSelector("charmmeta"))
	c.Assert(err, gc.Equals, nil)
	r, err := store.OpenCachedBlobFile(entity, "", nil)
	c.Assert(err, gc.ErrorMatches, "provided entity does not have required fields")
	c.Assert(r, gc.Equals, nil)
}

func (s *StoreSuite) TestOpenCachedBlobFileWithFoundContent(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()

	wordpress := storetesting.Charms.CharmDir("wordpress")
	url := router.MustNewResolvedURL("cs:~charmers/"+storetesting.SearchSeries[0]+"/wordpress-23", 23)
	err := store.AddCharmWithArchive(url, wordpress)
	c.Assert(err, gc.Equals, nil)

	// Get our expected content.
	data, err := ioutil.ReadFile(filepath.Join(wordpress.Path, "metadata.yaml"))
	c.Assert(err, gc.Equals, nil)
	expectContent := string(data)

	entity, err := store.FindEntity(url, FieldSelector("blobhash", "contents"))
	c.Assert(err, gc.Equals, nil)

	// Check that, when we open the file for the first time,
	// we see the expected content.
	r, err := store.OpenCachedBlobFile(entity, mongodoc.FileIcon, func(f *zip.File) bool {
		return path.Clean(f.Name) == "metadata.yaml"
	})
	c.Assert(err, gc.Equals, nil)
	defer r.Close()
	data, err = ioutil.ReadAll(r)
	c.Assert(err, gc.Equals, nil)
	c.Assert(string(data), gc.Equals, expectContent)

	// When retrieving the entity again, check that the Contents
	// map has been set appropriately...
	entity, err = store.FindEntity(url, FieldSelector("blobhash", "contents"))
	c.Assert(err, gc.Equals, nil)
	c.Assert(entity.Contents, gc.HasLen, 1)
	c.Assert(entity.Contents[mongodoc.FileIcon].IsValid(), gc.Equals, true)

	// ... and that OpenCachedBlobFile still returns a reader with the
	// same data, without making use of the isFile callback.
	r, err = store.OpenCachedBlobFile(entity, mongodoc.FileIcon, func(f *zip.File) bool {
		c.Errorf("isFile called unexpectedly")
		return false
	})
	c.Assert(err, gc.Equals, nil)
	defer r.Close()
	data, err = ioutil.ReadAll(r)
	c.Assert(err, gc.Equals, nil)
	c.Assert(string(data), gc.Equals, expectContent)
}

func (s *StoreSuite) TestOpenCachedBlobFileWithNotFoundContent(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()

	wordpress := storetesting.Charms.CharmDir("wordpress")
	url := router.MustNewResolvedURL("cs:~charmers/"+storetesting.SearchSeries[0]+"/wordpress-23", 23)
	err := store.AddCharmWithArchive(url, wordpress)
	c.Assert(err, gc.Equals, nil)

	entity, err := store.FindEntity(url, FieldSelector("blobhash", "contents"))
	c.Assert(err, gc.Equals, nil)

	// Check that, when we open the file for the first time,
	// we get a NotFound error.
	r, err := store.OpenCachedBlobFile(entity, mongodoc.FileIcon, func(f *zip.File) bool {
		return false
	})
	c.Assert(err, gc.ErrorMatches, "not found")
	c.Assert(errgo.Cause(err), gc.Equals, params.ErrNotFound)
	c.Assert(r, gc.Equals, nil)

	// When retrieving the entity again, check that the Contents
	// map has been set appropriately...
	entity, err = store.FindEntity(url, FieldSelector("blobhash", "contents"))
	c.Assert(err, gc.Equals, nil)
	c.Assert(entity.Contents, gc.DeepEquals, map[mongodoc.FileId]mongodoc.ZipFile{
		mongodoc.FileIcon: {},
	})

	// ... and that OpenCachedBlobFile still returns a NotFound
	// error, without making use of the isFile callback.
	r, err = store.OpenCachedBlobFile(entity, mongodoc.FileIcon, func(f *zip.File) bool {
		c.Errorf("isFile called unexpectedly")
		return false
	})
	c.Assert(err, gc.ErrorMatches, "not found")
	c.Assert(errgo.Cause(err), gc.Equals, params.ErrNotFound)
	c.Assert(r, gc.Equals, nil)
}

func (s *StoreSuite) TestSESPutDoesNotErrorWithNoESConfigured(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()
	err := store.UpdateSearch(nil)
	c.Assert(err, gc.Equals, nil)
}

var findBestEntityCharms = []struct {
	id     *router.ResolvedURL
	charm  charm.Charm
	edge   bool
	stable bool
}{{
	id:     router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/wordpress-0", 0),
	charm:  storetesting.NewCharm(nil),
	edge:   true,
	stable: true,
}, {
	id:     router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/wordpress-1", 1),
	charm:  storetesting.NewCharm(nil),
	edge:   true,
	stable: true,
}, {
	id:     router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/wordpress-2", 2),
	charm:  storetesting.NewCharm(nil),
	edge:   true,
	stable: false,
}, {
	id:     router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/wordpress-3", 3),
	charm:  storetesting.NewCharm(nil),
	edge:   false,
	stable: false,
}, {
	id:     router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[2]+"/wordpress-4", 4),
	charm:  storetesting.NewCharm(nil),
	edge:   true,
	stable: true,
}, {
	id:     router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[2]+"/wordpress-5", 5),
	charm:  storetesting.NewCharm(nil),
	edge:   true,
	stable: true,
}, {
	id:     router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[2]+"/wordpress-6", 6),
	charm:  storetesting.NewCharm(nil),
	edge:   true,
	stable: false,
}, {
	id:     router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[2]+"/wordpress-7", 7),
	charm:  storetesting.NewCharm(nil),
	edge:   false,
	stable: false,
}, {
	id:     router.MustNewResolvedURL("~charmers/mysql-0", 0),
	charm:  storetesting.NewCharm(storetesting.MetaWithSupportedSeries(nil, storetesting.SearchSeries[1], storetesting.SearchSeries[2])),
	edge:   true,
	stable: true,
}, {
	id:     router.MustNewResolvedURL("~charmers/mysql-1", 1),
	charm:  storetesting.NewCharm(storetesting.MetaWithSupportedSeries(nil, storetesting.SearchSeries[1], storetesting.SearchSeries[2])),
	edge:   true,
	stable: true,
}, {
	id:     router.MustNewResolvedURL("~charmers/mysql-2", 2),
	charm:  storetesting.NewCharm(storetesting.MetaWithSupportedSeries(nil, storetesting.SearchSeries[1], storetesting.SearchSeries[2])),
	edge:   true,
	stable: false,
}, {
	id:     router.MustNewResolvedURL("~charmers/mysql-3", 3),
	charm:  storetesting.NewCharm(storetesting.MetaWithSupportedSeries(nil, storetesting.SearchSeries[1], storetesting.SearchSeries[2])),
	edge:   false,
	stable: false,
}, {
	id:     router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/mongodb-0", -1),
	charm:  storetesting.NewCharm(nil),
	edge:   true,
	stable: true,
}, {
	id:     router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/mongodb-1", -1),
	charm:  storetesting.NewCharm(nil),
	edge:   true,
	stable: true,
}, {
	id:     router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/mongodb-2", -1),
	charm:  storetesting.NewCharm(nil),
	edge:   true,
	stable: false,
}, {
	id:     router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/mongodb-3", -1),
	charm:  storetesting.NewCharm(nil),
	edge:   false,
	stable: false,
}, {
	id:     router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/apache-0", 0),
	charm:  storetesting.NewCharm(nil),
	edge:   true,
	stable: false,
}, {
	id:     router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/nginx-0", 0),
	charm:  storetesting.NewCharm(nil),
	edge:   false,
	stable: false,
}, {
	id:     router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/postgresql-0", 0),
	charm:  storetesting.NewCharm(nil),
	edge:   true,
	stable: true,
}, {
	id:     router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[2]+"/postgresql-0", 0),
	charm:  storetesting.NewCharm(nil),
	edge:   true,
	stable: true,
}, {
	id:     router.MustNewResolvedURL("~charmers/postgresql-1", 1),
	charm:  storetesting.NewCharm(storetesting.MetaWithSupportedSeries(nil, storetesting.SearchSeries[0], storetesting.SearchSeries[1])),
	edge:   true,
	stable: true,
}, {
	id:     router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/ceph-0", 0),
	charm:  storetesting.NewCharm(nil),
	edge:   true,
	stable: true,
}, {
	id:     router.MustNewResolvedURL("~openstack-charmers/"+storetesting.SearchSeries[1]+"/ceph-0", 1),
	charm:  storetesting.NewCharm(nil),
	edge:   true,
	stable: false,
}, {
	id:     router.MustNewResolvedURL("~charmers/elasticsearch-0", -1),
	charm:  storetesting.NewCharm(storetesting.MetaWithSupportedSeries(nil, storetesting.SearchSeries[2])),
	edge:   true,
	stable: true,
}, {
	id:     router.MustNewResolvedURL("~charmers/elasticsearch-1", -1),
	charm:  storetesting.NewCharm(storetesting.MetaWithSupportedSeries(nil, storetesting.SearchSeries[1])),
	edge:   true,
	stable: true,
}}

var findBestEntityBundles = []struct {
	id     *router.ResolvedURL
	bundle charm.Bundle
	edge   bool
	stable bool
}{{
	id: router.MustNewResolvedURL("~charmers/bundle/wordpress-simple-0", 0),
	bundle: storetesting.NewBundle(&charm.BundleData{
		Applications: map[string]*charm.ApplicationSpec{
			"wordpress": {
				Charm: "cs:wordpress",
			},
			"mysql": {
				Charm: "cs:mysql",
			},
		},
	}),
	edge:   true,
	stable: true,
}, {
	id: router.MustNewResolvedURL("~charmers/bundle/wordpress-simple-1", 1),
	bundle: storetesting.NewBundle(&charm.BundleData{
		Applications: map[string]*charm.ApplicationSpec{
			"wordpress": {
				Charm: "cs:wordpress",
			},
			"mysql": {
				Charm: "cs:mysql",
			},
		},
	}),
	edge:   true,
	stable: true,
}, {
	id: router.MustNewResolvedURL("~charmers/bundle/wordpress-simple-2", 2),
	bundle: storetesting.NewBundle(&charm.BundleData{
		Applications: map[string]*charm.ApplicationSpec{
			"wordpress": {
				Charm: "cs:wordpress",
			},
			"mysql": {
				Charm: "cs:mysql",
			},
		},
	}),
	edge:   true,
	stable: false,
}, {
	id: router.MustNewResolvedURL("~charmers/bundle/wordpress-simple-3", 3),
	bundle: storetesting.NewBundle(&charm.BundleData{
		Applications: map[string]*charm.ApplicationSpec{
			"wordpress": {
				Charm: "cs:wordpress",
			},
			"mysql": {
				Charm: "cs:mysql",
			},
		},
	}),
	edge:   false,
	stable: false,
}}

var findBestEntityTests = []struct {
	url              string
	channel          params.Channel
	expectID         *router.ResolvedURL
	expectError      string
	expectErrorCause error
}{{
	url:      "~charmers/" + storetesting.SearchSeries[1] + "/wordpress-0",
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/wordpress-0", 0),
}, {
	url:      "~charmers/" + storetesting.SearchSeries[1] + "/wordpress-0",
	channel:  params.StableChannel,
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/wordpress-0", 0),
}, {
	url:      "~charmers/" + storetesting.SearchSeries[1] + "/wordpress-0",
	channel:  params.EdgeChannel,
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/wordpress-0", 0),
}, {
	url:      "~charmers/" + storetesting.SearchSeries[1] + "/wordpress-0",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/wordpress-0", 0),
}, {
	url:      "~charmers/" + storetesting.SearchSeries[1] + "/wordpress-3",
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/wordpress-3", 3),
}, {
	url:      "~charmers/" + storetesting.SearchSeries[1] + "/wordpress-3",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/wordpress-3", 3),
}, {
	url:              "~charmers/" + storetesting.SearchSeries[1] + "/wordpress-3",
	channel:          params.EdgeChannel,
	expectError:      "cs:~charmers/" + storetesting.SearchSeries[1] + "/wordpress-3 not found in edge channel",
	expectErrorCause: params.ErrNotFound,
}, {
	url:              "~charmers/" + storetesting.SearchSeries[1] + "/wordpress-2",
	channel:          params.StableChannel,
	expectError:      "cs:~charmers/" + storetesting.SearchSeries[1] + "/wordpress-2 not found in stable channel",
	expectErrorCause: params.ErrNotFound,
}, {
	url:      storetesting.SearchSeries[1] + "/wordpress-0",
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/wordpress-0", 0),
}, {
	url:      storetesting.SearchSeries[1] + "/wordpress-0",
	channel:  params.StableChannel,
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/wordpress-0", 0),
}, {
	url:      storetesting.SearchSeries[1] + "/wordpress-0",
	channel:  params.EdgeChannel,
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/wordpress-0", 0),
}, {
	url:      storetesting.SearchSeries[1] + "/wordpress-0",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/wordpress-0", 0),
}, {
	url:      storetesting.SearchSeries[1] + "/wordpress-3",
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/wordpress-3", 3),
}, {
	url:      storetesting.SearchSeries[1] + "/wordpress-3",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/wordpress-3", 3),
}, {
	url:              storetesting.SearchSeries[1] + "/wordpress-3",
	channel:          params.EdgeChannel,
	expectError:      "cs:" + storetesting.SearchSeries[1] + "/wordpress-3 not found in edge channel",
	expectErrorCause: params.ErrNotFound,
}, {
	url:              storetesting.SearchSeries[1] + "/wordpress-2",
	channel:          params.StableChannel,
	expectError:      "cs:" + storetesting.SearchSeries[1] + "/wordpress-2 not found in stable channel",
	expectErrorCause: params.ErrNotFound,
}, {
	url:      "~charmers/" + storetesting.SearchSeries[1] + "/wordpress",
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/wordpress-1", 1),
}, {
	url:      "~charmers/" + storetesting.SearchSeries[1] + "/wordpress",
	channel:  params.StableChannel,
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/wordpress-1", 1),
}, {
	url:      "~charmers/" + storetesting.SearchSeries[1] + "/wordpress",
	channel:  params.EdgeChannel,
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/wordpress-2", 2),
}, {
	url:      "~charmers/" + storetesting.SearchSeries[1] + "/wordpress",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/wordpress-3", 3),
}, {
	url:      storetesting.SearchSeries[1] + "/wordpress",
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/wordpress-1", 1),
}, {
	url:      storetesting.SearchSeries[1] + "/wordpress",
	channel:  params.StableChannel,
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/wordpress-1", 1),
}, {
	url:      storetesting.SearchSeries[1] + "/wordpress",
	channel:  params.EdgeChannel,
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/wordpress-2", 2),
}, {
	url:      storetesting.SearchSeries[1] + "/wordpress",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/wordpress-3", 3),
}, {
	url:      storetesting.SearchSeries[2] + "/wordpress",
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[2]+"/wordpress-5", 5),
}, {
	url:      storetesting.SearchSeries[2] + "/wordpress",
	channel:  params.StableChannel,
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[2]+"/wordpress-5", 5),
}, {
	url:      storetesting.SearchSeries[2] + "/wordpress",
	channel:  params.EdgeChannel,
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[2]+"/wordpress-6", 6),
}, {
	url:      storetesting.SearchSeries[2] + "/wordpress",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[2]+"/wordpress-7", 7),
}, {
	url:      "wordpress",
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/wordpress-1", 1),
}, {
	url:      "wordpress",
	channel:  params.StableChannel,
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/wordpress-1", 1),
}, {
	url:      "wordpress",
	channel:  params.EdgeChannel,
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/wordpress-2", 2),
}, {
	url:      "wordpress",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/wordpress-3", 3),
}, {
	url:      "~charmers/wordpress",
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/wordpress-1", 1),
}, {
	url:      "~charmers/wordpress",
	channel:  params.StableChannel,
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/wordpress-1", 1),
}, {
	url:      "~charmers/wordpress",
	channel:  params.EdgeChannel,
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/wordpress-2", 2),
}, {
	url:      "~charmers/wordpress",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/wordpress-3", 3),
}, {
	url:              "~charmers/wordpress-0",
	expectError:      "no matching charm or bundle for cs:~charmers/wordpress-0",
	expectErrorCause: params.ErrNotFound,
}, {
	url:              "~charmers/wordpress-0",
	channel:          params.StableChannel,
	expectError:      "no matching charm or bundle for cs:~charmers/wordpress-0",
	expectErrorCause: params.ErrNotFound,
}, {
	url:              "~charmers/wordpress-0",
	channel:          params.EdgeChannel,
	expectError:      "no matching charm or bundle for cs:~charmers/wordpress-0",
	expectErrorCause: params.ErrNotFound,
}, {
	url:              "~charmers/wordpress-0",
	channel:          params.UnpublishedChannel,
	expectError:      "no matching charm or bundle for cs:~charmers/wordpress-0",
	expectErrorCause: params.ErrNotFound,
}, {
	url:      "~charmers/mysql-0",
	expectID: router.MustNewResolvedURL("~charmers/mysql-0", 0),
}, {
	url:      "~charmers/mysql-0",
	channel:  params.StableChannel,
	expectID: router.MustNewResolvedURL("~charmers/mysql-0", 0),
}, {
	url:      "~charmers/mysql-0",
	channel:  params.EdgeChannel,
	expectID: router.MustNewResolvedURL("~charmers/mysql-0", 0),
}, {
	url:      "~charmers/mysql-0",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/mysql-0", 0),
}, {
	url:      "~charmers/mysql-3",
	expectID: router.MustNewResolvedURL("~charmers/mysql-3", 3),
}, {
	url:      "~charmers/mysql-3",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/mysql-3", 3),
}, {
	url:              "~charmers/mysql-3",
	channel:          params.EdgeChannel,
	expectError:      "cs:~charmers/mysql-3 not found in edge channel",
	expectErrorCause: params.ErrNotFound,
}, {
	url:              "~charmers/mysql-2",
	channel:          params.StableChannel,
	expectError:      "cs:~charmers/mysql-2 not found in stable channel",
	expectErrorCause: params.ErrNotFound,
}, {
	url:      "mysql-0",
	expectID: findBestEntityCharms[8].id,
}, {
	url:      "mysql-0",
	channel:  params.StableChannel,
	expectID: router.MustNewResolvedURL("~charmers/mysql-0", 0),
}, {
	url:      "mysql-0",
	channel:  params.EdgeChannel,
	expectID: router.MustNewResolvedURL("~charmers/mysql-0", 0),
}, {
	url:      "mysql-0",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/mysql-0", 0),
}, {
	url:      "mysql-3",
	expectID: router.MustNewResolvedURL("~charmers/mysql-3", 3),
}, {
	url:      "mysql-3",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/mysql-3", 3),
}, {
	url:              "mysql-3",
	channel:          params.EdgeChannel,
	expectError:      "cs:mysql-3 not found in edge channel",
	expectErrorCause: params.ErrNotFound,
}, {
	url:              "mysql-2",
	channel:          params.StableChannel,
	expectError:      "cs:mysql-2 not found in stable channel",
	expectErrorCause: params.ErrNotFound,
}, {
	url:      "~charmers/mysql",
	expectID: router.MustNewResolvedURL("~charmers/mysql-1", 1),
}, {
	url:      "~charmers/mysql",
	channel:  params.StableChannel,
	expectID: router.MustNewResolvedURL("~charmers/mysql-1", 1),
}, {
	url:      "~charmers/mysql",
	channel:  params.EdgeChannel,
	expectID: router.MustNewResolvedURL("~charmers/mysql-2", 2),
}, {
	url:      "~charmers/mysql",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/mysql-3", 3),
}, {
	url:      "mysql",
	expectID: router.MustNewResolvedURL("~charmers/mysql-1", 1),
}, {
	url:      "mysql",
	channel:  params.StableChannel,
	expectID: router.MustNewResolvedURL("~charmers/mysql-1", 1),
}, {
	url:      "mysql",
	channel:  params.EdgeChannel,
	expectID: router.MustNewResolvedURL("~charmers/mysql-2", 2),
}, {
	url:      "mysql",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/mysql-3", 3),
}, {
	url:      "~charmers/" + storetesting.SearchSeries[1] + "/mysql",
	expectID: router.MustNewResolvedURL("~charmers/mysql-1", 1),
}, {
	url:      "~charmers/" + storetesting.SearchSeries[1] + "/mysql",
	channel:  params.StableChannel,
	expectID: router.MustNewResolvedURL("~charmers/mysql-1", 1),
}, {
	url:      "~charmers/" + storetesting.SearchSeries[1] + "/mysql",
	channel:  params.EdgeChannel,
	expectID: router.MustNewResolvedURL("~charmers/mysql-2", 2),
}, {
	url:      "~charmers/" + storetesting.SearchSeries[1] + "/mysql",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/mysql-3", 3),
}, {
	url:      storetesting.SearchSeries[1] + "/mysql",
	expectID: router.MustNewResolvedURL("~charmers/mysql-1", 1),
}, {
	url:      storetesting.SearchSeries[1] + "/mysql",
	channel:  params.StableChannel,
	expectID: router.MustNewResolvedURL("~charmers/mysql-1", 1),
}, {
	url:      storetesting.SearchSeries[1] + "/mysql",
	channel:  params.EdgeChannel,
	expectID: router.MustNewResolvedURL("~charmers/mysql-2", 2),
}, {
	url:      storetesting.SearchSeries[1] + "/mysql",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/mysql-3", 3),
}, {
	url:      "~charmers/" + storetesting.SearchSeries[2] + "/mysql",
	expectID: router.MustNewResolvedURL("~charmers/mysql-1", 1),
}, {
	url:      "~charmers/" + storetesting.SearchSeries[2] + "/mysql",
	channel:  params.StableChannel,
	expectID: router.MustNewResolvedURL("~charmers/mysql-1", 1),
}, {
	url:      "~charmers/" + storetesting.SearchSeries[2] + "/mysql",
	channel:  params.EdgeChannel,
	expectID: router.MustNewResolvedURL("~charmers/mysql-2", 2),
}, {
	url:      "~charmers/" + storetesting.SearchSeries[2] + "/mysql",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/mysql-3", 3),
}, {
	url:      storetesting.SearchSeries[2] + "/mysql",
	expectID: router.MustNewResolvedURL("~charmers/mysql-1", 1),
}, {
	url:      storetesting.SearchSeries[2] + "/mysql",
	channel:  params.StableChannel,
	expectID: router.MustNewResolvedURL("~charmers/mysql-1", 1),
}, {
	url:      storetesting.SearchSeries[2] + "/mysql",
	channel:  params.EdgeChannel,
	expectID: router.MustNewResolvedURL("~charmers/mysql-2", 2),
}, {
	url:      storetesting.SearchSeries[2] + "/mysql",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/mysql-3", 3),
}, {
	url:      "~charmers/" + storetesting.SearchSeries[1] + "/mongodb-0",
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/mongodb-0", -1),
}, {
	url:      "~charmers/" + storetesting.SearchSeries[1] + "/mongodb-0",
	channel:  params.StableChannel,
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/mongodb-0", -1),
}, {
	url:      "~charmers/" + storetesting.SearchSeries[1] + "/mongodb-0",
	channel:  params.EdgeChannel,
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/mongodb-0", -1),
}, {
	url:      "~charmers/" + storetesting.SearchSeries[1] + "/mongodb-0",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/mongodb-0", -1),
}, {
	url:      "~charmers/" + storetesting.SearchSeries[1] + "/mongodb-3",
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/mongodb-3", -1),
}, {
	url:      "~charmers/" + storetesting.SearchSeries[1] + "/mongodb-3",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/mongodb-3", -1),
}, {
	url:              "~charmers/" + storetesting.SearchSeries[1] + "/mongodb-3",
	channel:          params.EdgeChannel,
	expectError:      "cs:~charmers/" + storetesting.SearchSeries[1] + "/mongodb-3 not found in edge channel",
	expectErrorCause: params.ErrNotFound,
}, {
	url:              "~charmers/" + storetesting.SearchSeries[1] + "/mongodb-2",
	channel:          params.StableChannel,
	expectError:      "cs:~charmers/" + storetesting.SearchSeries[1] + "/mongodb-2 not found in stable channel",
	expectErrorCause: params.ErrNotFound,
}, {
	url:              storetesting.SearchSeries[1] + "/mongodb-0",
	expectError:      "no matching charm or bundle for cs:" + storetesting.SearchSeries[1] + "/mongodb-0",
	expectErrorCause: params.ErrNotFound,
}, {
	url:              storetesting.SearchSeries[1] + "/mongodb-0",
	channel:          params.StableChannel,
	expectError:      "no matching charm or bundle for cs:" + storetesting.SearchSeries[1] + "/mongodb-0",
	expectErrorCause: params.ErrNotFound,
}, {
	url:              storetesting.SearchSeries[1] + "/mongodb-0",
	channel:          params.EdgeChannel,
	expectError:      "no matching charm or bundle for cs:" + storetesting.SearchSeries[1] + "/mongodb-0",
	expectErrorCause: params.ErrNotFound,
}, {
	url:              storetesting.SearchSeries[1] + "/mongodb-0",
	channel:          params.UnpublishedChannel,
	expectError:      "no matching charm or bundle for cs:" + storetesting.SearchSeries[1] + "/mongodb-0",
	expectErrorCause: params.ErrNotFound,
}, {
	url:      "~charmers/" + storetesting.SearchSeries[1] + "/mongodb",
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/mongodb-1", -1),
}, {
	url:      "~charmers/" + storetesting.SearchSeries[1] + "/mongodb",
	channel:  params.StableChannel,
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/mongodb-1", -1),
}, {
	url:      "~charmers/" + storetesting.SearchSeries[1] + "/mongodb",
	channel:  params.EdgeChannel,
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/mongodb-2", -1),
}, {
	url:      "~charmers/" + storetesting.SearchSeries[1] + "/mongodb",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/mongodb-3", -1),
}, {
	url:              storetesting.SearchSeries[1] + "/mongodb",
	expectError:      "no matching charm or bundle for cs:" + storetesting.SearchSeries[1] + "/mongodb",
	expectErrorCause: params.ErrNotFound,
}, {
	url:              storetesting.SearchSeries[1] + "/mongodb",
	channel:          params.StableChannel,
	expectError:      "no matching charm or bundle for cs:" + storetesting.SearchSeries[1] + "/mongodb",
	expectErrorCause: params.ErrNotFound,
}, {
	url:              storetesting.SearchSeries[1] + "/mongodb",
	channel:          params.EdgeChannel,
	expectError:      "no matching charm or bundle for cs:" + storetesting.SearchSeries[1] + "/mongodb",
	expectErrorCause: params.ErrNotFound,
}, {
	url:              storetesting.SearchSeries[1] + "/mongodb",
	channel:          params.UnpublishedChannel,
	expectError:      "no matching charm or bundle for cs:" + storetesting.SearchSeries[1] + "/mongodb",
	expectErrorCause: params.ErrNotFound,
}, {
	url:              "mongodb",
	expectError:      "no matching charm or bundle for cs:mongodb",
	expectErrorCause: params.ErrNotFound,
}, {
	url:              "mongodb",
	channel:          params.StableChannel,
	expectError:      "no matching charm or bundle for cs:mongodb",
	expectErrorCause: params.ErrNotFound,
}, {
	url:              "mongodb",
	channel:          params.EdgeChannel,
	expectError:      "no matching charm or bundle for cs:mongodb",
	expectErrorCause: params.ErrNotFound,
}, {
	url:              "mongodb",
	channel:          params.UnpublishedChannel,
	expectError:      "no matching charm or bundle for cs:mongodb",
	expectErrorCause: params.ErrNotFound,
}, {
	url:              "~charmers/" + storetesting.SearchSeries[1] + "/apache",
	expectError:      "no matching charm or bundle for cs:~charmers/" + storetesting.SearchSeries[1] + "/apache",
	expectErrorCause: params.ErrNotFound,
}, {
	url:              "~charmers/" + storetesting.SearchSeries[1] + "/apache",
	channel:          params.StableChannel,
	expectError:      "no matching charm or bundle for cs:~charmers/" + storetesting.SearchSeries[1] + "/apache",
	expectErrorCause: params.ErrNotFound,
}, {
	url:      "~charmers/" + storetesting.SearchSeries[1] + "/apache",
	channel:  params.EdgeChannel,
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/apache-0", 0),
}, {
	url:      "~charmers/" + storetesting.SearchSeries[1] + "/apache",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/apache-0", 0),
}, {
	url:      "~charmers/" + storetesting.SearchSeries[1] + "/apache-0",
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/apache-0", 0),
}, {
	url:         "~charmers/" + storetesting.SearchSeries[1] + "/apache-0",
	channel:     params.StableChannel,
	expectError: "cs:~charmers/" + storetesting.SearchSeries[1] + "/apache-0 not found in stable channel",
}, {
	url:      "~charmers/" + storetesting.SearchSeries[1] + "/apache-0",
	channel:  params.EdgeChannel,
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/apache-0", 0),
}, {
	url:      "~charmers/" + storetesting.SearchSeries[1] + "/apache-0",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/apache-0", 0),
}, {
	url:              storetesting.SearchSeries[1] + "/apache",
	expectError:      "no matching charm or bundle for cs:" + storetesting.SearchSeries[1] + "/apache",
	expectErrorCause: params.ErrNotFound,
}, {
	url:              storetesting.SearchSeries[1] + "/apache",
	channel:          params.StableChannel,
	expectError:      "no matching charm or bundle for cs:" + storetesting.SearchSeries[1] + "/apache",
	expectErrorCause: params.ErrNotFound,
}, {
	url:      storetesting.SearchSeries[1] + "/apache",
	channel:  params.EdgeChannel,
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/apache-0", 0),
}, {
	url:      storetesting.SearchSeries[1] + "/apache",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/apache-0", 0),
}, {
	url:      storetesting.SearchSeries[1] + "/apache-0",
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/apache-0", 0),
}, {
	url:         storetesting.SearchSeries[1] + "/apache-0",
	channel:     params.StableChannel,
	expectError: "cs:" + storetesting.SearchSeries[1] + "/apache-0 not found in stable channel",
}, {
	url:      storetesting.SearchSeries[1] + "/apache-0",
	channel:  params.EdgeChannel,
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/apache-0", 0),
}, {
	url:      storetesting.SearchSeries[1] + "/apache-0",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/apache-0", 0),
}, {
	url:              "~charmers/" + storetesting.SearchSeries[1] + "/nginx",
	expectError:      "no matching charm or bundle for cs:~charmers/" + storetesting.SearchSeries[1] + "/nginx",
	expectErrorCause: params.ErrNotFound,
}, {
	url:              "~charmers/" + storetesting.SearchSeries[1] + "/nginx",
	channel:          params.StableChannel,
	expectError:      "no matching charm or bundle for cs:~charmers/" + storetesting.SearchSeries[1] + "/nginx",
	expectErrorCause: params.ErrNotFound,
}, {
	url:              "~charmers/" + storetesting.SearchSeries[1] + "/nginx",
	channel:          params.EdgeChannel,
	expectError:      "no matching charm or bundle for cs:~charmers/" + storetesting.SearchSeries[1] + "/nginx",
	expectErrorCause: params.ErrNotFound,
}, {
	url:      "~charmers/" + storetesting.SearchSeries[1] + "/nginx",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/nginx-0", 0),
}, {
	url:      "~charmers/" + storetesting.SearchSeries[1] + "/nginx-0",
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/nginx-0", 0),
}, {
	url:         "~charmers/" + storetesting.SearchSeries[1] + "/nginx-0",
	channel:     params.StableChannel,
	expectError: "cs:~charmers/" + storetesting.SearchSeries[1] + "/nginx-0 not found in stable channel",
}, {
	url:         "~charmers/" + storetesting.SearchSeries[1] + "/nginx-0",
	channel:     params.EdgeChannel,
	expectError: "cs:~charmers/" + storetesting.SearchSeries[1] + "/nginx-0 not found in edge channel",
}, {
	url:      "~charmers/" + storetesting.SearchSeries[1] + "/nginx-0",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/nginx-0", 0),
}, {
	url:              storetesting.SearchSeries[1] + "/nginx",
	expectError:      "no matching charm or bundle for cs:" + storetesting.SearchSeries[1] + "/nginx",
	expectErrorCause: params.ErrNotFound,
}, {
	url:              storetesting.SearchSeries[1] + "/nginx",
	channel:          params.StableChannel,
	expectError:      "no matching charm or bundle for cs:" + storetesting.SearchSeries[1] + "/nginx",
	expectErrorCause: params.ErrNotFound,
}, {
	url:              storetesting.SearchSeries[1] + "/nginx",
	channel:          params.EdgeChannel,
	expectError:      "no matching charm or bundle for cs:" + storetesting.SearchSeries[1] + "/nginx",
	expectErrorCause: params.ErrNotFound,
}, {
	url:      storetesting.SearchSeries[1] + "/nginx",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/nginx-0", 0),
}, {
	url:      storetesting.SearchSeries[1] + "/nginx-0",
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/nginx-0", 0),
}, {
	url:         storetesting.SearchSeries[1] + "/nginx-0",
	channel:     params.StableChannel,
	expectError: "cs:" + storetesting.SearchSeries[1] + "/nginx-0 not found in stable channel",
}, {
	url:         storetesting.SearchSeries[1] + "/nginx-0",
	channel:     params.EdgeChannel,
	expectError: "cs:" + storetesting.SearchSeries[1] + "/nginx-0 not found in edge channel",
}, {
	url:      storetesting.SearchSeries[1] + "/nginx-0",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/nginx-0", 0),
}, {
	url:      "~charmers/bundle/wordpress-simple-0",
	expectID: router.MustNewResolvedURL("~charmers/bundle/wordpress-simple-0", 0),
}, {
	url:      "~charmers/bundle/wordpress-simple-0",
	channel:  params.StableChannel,
	expectID: router.MustNewResolvedURL("~charmers/bundle/wordpress-simple-0", 0),
}, {
	url:      "~charmers/bundle/wordpress-simple-0",
	channel:  params.EdgeChannel,
	expectID: router.MustNewResolvedURL("~charmers/bundle/wordpress-simple-0", 0),
}, {
	url:      "~charmers/bundle/wordpress-simple-0",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/bundle/wordpress-simple-0", 0),
}, {
	url:      "~charmers/bundle/wordpress-simple-3",
	expectID: router.MustNewResolvedURL("~charmers/bundle/wordpress-simple-3", 3),
}, {
	url:      "~charmers/bundle/wordpress-simple-3",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/bundle/wordpress-simple-3", 3),
}, {
	url:         "~charmers/bundle/wordpress-simple-3",
	channel:     params.EdgeChannel,
	expectError: "cs:~charmers/bundle/wordpress-simple-3 not found in edge channel",
}, {
	url:         "~charmers/bundle/wordpress-simple-3",
	channel:     params.StableChannel,
	expectError: "cs:~charmers/bundle/wordpress-simple-3 not found in stable channel",
}, {
	url:      "bundle/wordpress-simple-0",
	expectID: router.MustNewResolvedURL("~charmers/bundle/wordpress-simple-0", 0),
}, {
	url:      "bundle/wordpress-simple-0",
	channel:  params.StableChannel,
	expectID: router.MustNewResolvedURL("~charmers/bundle/wordpress-simple-0", 0),
}, {
	url:      "bundle/wordpress-simple-0",
	channel:  params.EdgeChannel,
	expectID: router.MustNewResolvedURL("~charmers/bundle/wordpress-simple-0", 0),
}, {
	url:      "bundle/wordpress-simple-0",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/bundle/wordpress-simple-0", 0),
}, {
	url:      "bundle/wordpress-simple-3",
	expectID: router.MustNewResolvedURL("~charmers/bundle/wordpress-simple-3", 3),
}, {
	url:      "bundle/wordpress-simple-3",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/bundle/wordpress-simple-3", 3),
}, {
	url:         "bundle/wordpress-simple-3",
	channel:     params.EdgeChannel,
	expectError: "cs:bundle/wordpress-simple-3 not found in edge channel",
}, {
	url:         "bundle/wordpress-simple-2",
	channel:     params.StableChannel,
	expectError: "cs:bundle/wordpress-simple-2 not found in stable channel",
}, {
	url:      "~charmers/bundle/wordpress-simple",
	expectID: router.MustNewResolvedURL("~charmers/bundle/wordpress-simple-1", 1),
}, {
	url:      "~charmers/bundle/wordpress-simple",
	channel:  params.StableChannel,
	expectID: router.MustNewResolvedURL("~charmers/bundle/wordpress-simple-1", 1),
}, {
	url:      "~charmers/bundle/wordpress-simple",
	channel:  params.EdgeChannel,
	expectID: router.MustNewResolvedURL("~charmers/bundle/wordpress-simple-2", 2),
}, {
	url:      "~charmers/bundle/wordpress-simple",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/bundle/wordpress-simple-3", 3),
}, {
	url:      "bundle/wordpress-simple",
	expectID: router.MustNewResolvedURL("~charmers/bundle/wordpress-simple-1", 1),
}, {
	url:      "bundle/wordpress-simple",
	channel:  params.StableChannel,
	expectID: router.MustNewResolvedURL("~charmers/bundle/wordpress-simple-1", 1),
}, {
	url:      "bundle/wordpress-simple",
	channel:  params.EdgeChannel,
	expectID: router.MustNewResolvedURL("~charmers/bundle/wordpress-simple-2", 2),
}, {
	url:      "bundle/wordpress-simple",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/bundle/wordpress-simple-3", 3),
}, {
	url:      "wordpress-simple",
	expectID: router.MustNewResolvedURL("~charmers/bundle/wordpress-simple-1", 1),
}, {
	url:      "wordpress-simple",
	channel:  params.StableChannel,
	expectID: router.MustNewResolvedURL("~charmers/bundle/wordpress-simple-1", 1),
}, {
	url:      "wordpress-simple",
	channel:  params.EdgeChannel,
	expectID: router.MustNewResolvedURL("~charmers/bundle/wordpress-simple-2", 2),
}, {
	url:      "wordpress-simple",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/bundle/wordpress-simple-3", 3),
}, {
	url:      "~charmers/wordpress-simple",
	expectID: router.MustNewResolvedURL("~charmers/bundle/wordpress-simple-1", 1),
}, {
	url:      "~charmers/wordpress-simple",
	channel:  params.StableChannel,
	expectID: router.MustNewResolvedURL("~charmers/bundle/wordpress-simple-1", 1),
}, {
	url:      "~charmers/wordpress-simple",
	channel:  params.EdgeChannel,
	expectID: router.MustNewResolvedURL("~charmers/bundle/wordpress-simple-2", 2),
}, {
	url:      "~charmers/wordpress-simple",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/bundle/wordpress-simple-3", 3),
}, {
	url:      "~charmers/wordpress-simple-0",
	expectID: router.MustNewResolvedURL("~charmers/bundle/wordpress-simple-0", 0),
}, {
	url:      "~charmers/wordpress-simple-0",
	channel:  params.StableChannel,
	expectID: router.MustNewResolvedURL("~charmers/bundle/wordpress-simple-0", 0),
}, {
	url:      "~charmers/wordpress-simple-0",
	channel:  params.EdgeChannel,
	expectID: router.MustNewResolvedURL("~charmers/bundle/wordpress-simple-0", 0),
}, {
	url:      "~charmers/wordpress-simple-0",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/bundle/wordpress-simple-0", 0),
}, {
	url:              "~charmers/" + storetesting.SearchSeries[1] + "/wordpress",
	channel:          "no-such-channel",
	expectError:      "no matching charm or bundle for cs:~charmers/" + storetesting.SearchSeries[1] + "/wordpress",
	expectErrorCause: params.ErrNotFound,
}, {
	url:      "~charmers/" + storetesting.SearchSeries[1] + "/postgresql-0",
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/postgresql-0", 0),
}, {
	url:      "~charmers/" + storetesting.SearchSeries[1] + "/postgresql-0",
	channel:  params.StableChannel,
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/postgresql-0", 0),
}, {
	url:      "~charmers/" + storetesting.SearchSeries[1] + "/postgresql-0",
	channel:  params.EdgeChannel,
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/postgresql-0", 0),
}, {
	url:      "~charmers/" + storetesting.SearchSeries[1] + "/postgresql-0",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/postgresql-0", 0),
}, {
	url:      "~charmers/" + storetesting.SearchSeries[2] + "/postgresql-0",
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[2]+"/postgresql-0", 0),
}, {
	url:      "~charmers/" + storetesting.SearchSeries[2] + "/postgresql-0",
	channel:  params.StableChannel,
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[2]+"/postgresql-0", 0),
}, {
	url:      "~charmers/" + storetesting.SearchSeries[2] + "/postgresql-0",
	channel:  params.EdgeChannel,
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[2]+"/postgresql-0", 0),
}, {
	url:      "~charmers/" + storetesting.SearchSeries[2] + "/postgresql-0",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[2]+"/postgresql-0", 0),
}, {
	url:      "~charmers/" + storetesting.SearchSeries[1] + "/postgresql-1",
	expectID: router.MustNewResolvedURL("~charmers/postgresql-1", 1),
}, {
	url:      "~charmers/" + storetesting.SearchSeries[1] + "/postgresql-1",
	channel:  params.StableChannel,
	expectID: router.MustNewResolvedURL("~charmers/postgresql-1", 1),
}, {
	url:      "~charmers/" + storetesting.SearchSeries[1] + "/postgresql-1",
	channel:  params.EdgeChannel,
	expectID: router.MustNewResolvedURL("~charmers/postgresql-1", 1),
}, {
	url:      "~charmers/" + storetesting.SearchSeries[1] + "/postgresql-1",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/postgresql-1", 1),
}, {
	url:      "~charmers/" + storetesting.SearchSeries[0] + "/postgresql-1",
	expectID: router.MustNewResolvedURL("~charmers/postgresql-1", 1),
}, {
	url:      "~charmers/" + storetesting.SearchSeries[0] + "/postgresql-1",
	channel:  params.StableChannel,
	expectID: router.MustNewResolvedURL("~charmers/postgresql-1", 1),
}, {
	url:      "~charmers/" + storetesting.SearchSeries[0] + "/postgresql-1",
	channel:  params.EdgeChannel,
	expectID: router.MustNewResolvedURL("~charmers/postgresql-1", 1),
}, {
	url:      "~charmers/" + storetesting.SearchSeries[0] + "/postgresql-1",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/postgresql-1", 1),
}, {
	url:      "~charmers/" + storetesting.SearchSeries[1] + "/postgresql",
	expectID: router.MustNewResolvedURL("~charmers/postgresql-1", 1),
}, {
	url:      "~charmers/" + storetesting.SearchSeries[1] + "/postgresql",
	channel:  params.StableChannel,
	expectID: router.MustNewResolvedURL("~charmers/postgresql-1", 1),
}, {
	url:      "~charmers/" + storetesting.SearchSeries[1] + "/postgresql",
	channel:  params.EdgeChannel,
	expectID: router.MustNewResolvedURL("~charmers/postgresql-1", 1),
}, {
	url:      "~charmers/" + storetesting.SearchSeries[1] + "/postgresql",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/postgresql-1", 1),
}, {
	url:      "~charmers/" + storetesting.SearchSeries[0] + "/postgresql",
	expectID: router.MustNewResolvedURL("~charmers/postgresql-1", 1),
}, {
	url:      "~charmers/" + storetesting.SearchSeries[0] + "/postgresql",
	channel:  params.StableChannel,
	expectID: router.MustNewResolvedURL("~charmers/postgresql-1", 1),
}, {
	url:      "~charmers/" + storetesting.SearchSeries[0] + "/postgresql",
	channel:  params.EdgeChannel,
	expectID: router.MustNewResolvedURL("~charmers/postgresql-1", 1),
}, {
	url:      "~charmers/" + storetesting.SearchSeries[0] + "/postgresql",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/postgresql-1", 1),
}, {
	url:      storetesting.SearchSeries[1] + "/postgresql",
	expectID: router.MustNewResolvedURL("~charmers/postgresql-1", 1),
}, {
	url:      storetesting.SearchSeries[1] + "/postgresql",
	channel:  params.StableChannel,
	expectID: router.MustNewResolvedURL("~charmers/postgresql-1", 1),
}, {
	url:      storetesting.SearchSeries[1] + "/postgresql",
	channel:  params.EdgeChannel,
	expectID: router.MustNewResolvedURL("~charmers/postgresql-1", 1),
}, {
	url:      storetesting.SearchSeries[1] + "/postgresql",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/postgresql-1", 1),
}, {
	url:      storetesting.SearchSeries[0] + "/postgresql",
	expectID: router.MustNewResolvedURL("~charmers/postgresql-1", 1),
}, {
	url:      storetesting.SearchSeries[0] + "/postgresql",
	channel:  params.StableChannel,
	expectID: router.MustNewResolvedURL("~charmers/postgresql-1", 1),
}, {
	url:      storetesting.SearchSeries[0] + "/postgresql",
	channel:  params.EdgeChannel,
	expectID: router.MustNewResolvedURL("~charmers/postgresql-1", 1),
}, {
	url:      storetesting.SearchSeries[0] + "/postgresql",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/postgresql-1", 1),
}, {
	url:      "postgresql",
	expectID: router.MustNewResolvedURL("~charmers/postgresql-1", 1),
}, {
	url:      "postgresql",
	channel:  params.StableChannel,
	expectID: router.MustNewResolvedURL("~charmers/postgresql-1", 1),
}, {
	url:      "postgresql",
	channel:  params.EdgeChannel,
	expectID: router.MustNewResolvedURL("~charmers/postgresql-1", 1),
}, {
	url:      "postgresql",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/postgresql-1", 1),
}, {
	url:      "postgresql-1",
	expectID: router.MustNewResolvedURL("~charmers/postgresql-1", 1),
}, {
	url:      "postgresql-1",
	channel:  params.StableChannel,
	expectID: router.MustNewResolvedURL("~charmers/postgresql-1", 1),
}, {
	url:      "postgresql-1",
	channel:  params.EdgeChannel,
	expectID: router.MustNewResolvedURL("~charmers/postgresql-1", 1),
}, {
	url:      "postgresql-1",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/postgresql-1", 1),
}, {
	url:              "postgresql-0",
	expectError:      "no matching charm or bundle for cs:postgresql-0",
	expectErrorCause: params.ErrNotFound,
}, {
	url:              "postgresql-0",
	channel:          params.StableChannel,
	expectError:      "no matching charm or bundle for cs:postgresql-0",
	expectErrorCause: params.ErrNotFound,
}, {
	url:              "postgresql-0",
	channel:          params.EdgeChannel,
	expectError:      "no matching charm or bundle for cs:postgresql-0",
	expectErrorCause: params.ErrNotFound,
}, {
	url:              "postgresql-0",
	channel:          params.UnpublishedChannel,
	expectError:      "no matching charm or bundle for cs:postgresql-0",
	expectErrorCause: params.ErrNotFound,
}, {
	url:      "~charmers/" + storetesting.SearchSeries[1] + "/ceph-0",
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/ceph-0", 0),
}, {
	url:      "~charmers/" + storetesting.SearchSeries[1] + "/ceph-0",
	channel:  params.StableChannel,
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/ceph-0", 0),
}, {
	url:      "~charmers/" + storetesting.SearchSeries[1] + "/ceph-0",
	channel:  params.EdgeChannel,
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/ceph-0", 0),
}, {
	url:      "~charmers/" + storetesting.SearchSeries[1] + "/ceph-0",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/ceph-0", 0),
}, {
	url:      "~charmers/" + storetesting.SearchSeries[1] + "/ceph",
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/ceph-0", 0),
}, {
	url:      "~charmers/" + storetesting.SearchSeries[1] + "/ceph",
	channel:  params.StableChannel,
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/ceph-0", 0),
}, {
	url:      "~charmers/" + storetesting.SearchSeries[1] + "/ceph",
	channel:  params.EdgeChannel,
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/ceph-0", 0),
}, {
	url:      "~charmers/" + storetesting.SearchSeries[1] + "/ceph",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/ceph-0", 0),
}, {
	url:      "~openstack-charmers/" + storetesting.SearchSeries[1] + "/ceph-0",
	expectID: router.MustNewResolvedURL("~openstack-charmers/"+storetesting.SearchSeries[1]+"/ceph-0", 1),
}, {
	url:         "~openstack-charmers/" + storetesting.SearchSeries[1] + "/ceph-0",
	channel:     params.StableChannel,
	expectError: "cs:~openstack-charmers/" + storetesting.SearchSeries[1] + "/ceph-0 not found in stable channel",
}, {
	url:      "~openstack-charmers/" + storetesting.SearchSeries[1] + "/ceph-0",
	channel:  params.EdgeChannel,
	expectID: router.MustNewResolvedURL("~openstack-charmers/"+storetesting.SearchSeries[1]+"/ceph-0", 1),
}, {
	url:      "~openstack-charmers/" + storetesting.SearchSeries[1] + "/ceph-0",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~openstack-charmers/"+storetesting.SearchSeries[1]+"/ceph-0", 1),
}, {
	url:              "~openstack-charmers/" + storetesting.SearchSeries[1] + "/ceph",
	expectError:      "no matching charm or bundle for cs:~openstack-charmers/" + storetesting.SearchSeries[1] + "/ceph",
	expectErrorCause: params.ErrNotFound,
}, {
	url:              "~openstack-charmers/" + storetesting.SearchSeries[1] + "/ceph",
	channel:          params.StableChannel,
	expectError:      "no matching charm or bundle for cs:~openstack-charmers/" + storetesting.SearchSeries[1] + "/ceph",
	expectErrorCause: params.ErrNotFound,
}, {
	url:      "~openstack-charmers/" + storetesting.SearchSeries[1] + "/ceph",
	channel:  params.EdgeChannel,
	expectID: router.MustNewResolvedURL("~openstack-charmers/"+storetesting.SearchSeries[1]+"/ceph-0", 1),
}, {
	url:      "~openstack-charmers/" + storetesting.SearchSeries[1] + "/ceph",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~openstack-charmers/"+storetesting.SearchSeries[1]+"/ceph-0", 1),
}, {
	url:      storetesting.SearchSeries[1] + "/ceph-0",
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/ceph-0", 0),
}, {
	url:      storetesting.SearchSeries[1] + "/ceph-0",
	channel:  params.StableChannel,
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/ceph-0", 0),
}, {
	url:      storetesting.SearchSeries[1] + "/ceph-0",
	channel:  params.EdgeChannel,
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/ceph-0", 0),
}, {
	url:      storetesting.SearchSeries[1] + "/ceph-0",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/ceph-0", 0),
}, {
	url:      storetesting.SearchSeries[1] + "/ceph-1",
	expectID: router.MustNewResolvedURL("~openstack-charmers/"+storetesting.SearchSeries[1]+"/ceph-0", 1),
}, {
	url:         storetesting.SearchSeries[1] + "/ceph-1",
	channel:     params.StableChannel,
	expectError: "cs:" + storetesting.SearchSeries[1] + "/ceph-1 not found in stable channel",
}, {
	url:      storetesting.SearchSeries[1] + "/ceph-1",
	channel:  params.EdgeChannel,
	expectID: router.MustNewResolvedURL("~openstack-charmers/"+storetesting.SearchSeries[1]+"/ceph-0", 1),
}, {
	url:      storetesting.SearchSeries[1] + "/ceph-1",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~openstack-charmers/"+storetesting.SearchSeries[1]+"/ceph-0", 1),
}, {
	url:              storetesting.SearchSeries[1] + "/ceph",
	expectError:      "no matching charm or bundle for cs:" + storetesting.SearchSeries[1] + "/ceph",
	expectErrorCause: params.ErrNotFound,
}, {
	url:              storetesting.SearchSeries[1] + "/ceph",
	channel:          params.StableChannel,
	expectError:      "no matching charm or bundle for cs:" + storetesting.SearchSeries[1] + "/ceph",
	expectErrorCause: params.ErrNotFound,
}, {
	url:      storetesting.SearchSeries[1] + "/ceph",
	channel:  params.EdgeChannel,
	expectID: router.MustNewResolvedURL("~openstack-charmers/"+storetesting.SearchSeries[1]+"/ceph-0", 1),
}, {
	url:      storetesting.SearchSeries[1] + "/ceph",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~openstack-charmers/"+storetesting.SearchSeries[1]+"/ceph-0", 1),
}, {
	url:              "ceph",
	expectError:      "no matching charm or bundle for cs:ceph",
	expectErrorCause: params.ErrNotFound,
}, {
	url:              "ceph",
	channel:          params.StableChannel,
	expectError:      "no matching charm or bundle for cs:ceph",
	expectErrorCause: params.ErrNotFound,
}, {
	url:      "ceph",
	channel:  params.EdgeChannel,
	expectID: router.MustNewResolvedURL("~openstack-charmers/"+storetesting.SearchSeries[1]+"/ceph-0", 1),
}, {
	url:      "ceph",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~openstack-charmers/"+storetesting.SearchSeries[1]+"/ceph-0", 1),
}, {
	url:      "~charmers/elasticsearch",
	channel:  params.StableChannel,
	expectID: router.MustNewResolvedURL("~charmers/elasticsearch-1", -1),
}}

func (s *StoreSuite) TestFindBestEntity(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()
	for _, ch := range findBestEntityCharms {
		err := store.AddCharmWithArchive(ch.id, ch.charm)
		c.Assert(err, gc.Equals, nil)
		err = store.SetPromulgated(ch.id, ch.id.PromulgatedRevision != -1)
		c.Assert(err, gc.Equals, nil)
		if ch.edge {
			err := store.Publish(ch.id, nil, params.EdgeChannel)
			c.Assert(err, gc.Equals, nil)
		}
		if ch.stable {
			err := store.Publish(ch.id, nil, params.StableChannel)
			c.Assert(err, gc.Equals, nil)
		}
	}

	for _, b := range findBestEntityBundles {
		err := store.AddBundleWithArchive(b.id, b.bundle)
		c.Assert(err, gc.Equals, nil)
		err = store.SetPromulgated(b.id, b.id.PromulgatedRevision != -1)
		c.Assert(err, gc.Equals, nil)
		if b.edge {
			err := store.Publish(b.id, nil, params.EdgeChannel)
			c.Assert(err, gc.Equals, nil)
		}
		if b.stable {
			err := store.Publish(b.id, nil, params.StableChannel)
			c.Assert(err, gc.Equals, nil)
		}
	}

	for i, test := range findBestEntityTests {
		c.Logf("test %d: %s (%s)", i, test.url, test.channel)
		// Run FindBestEntity a number of times to make sure resolution is predicatable.
		for j := 0; j < 10; j++ {
			entity, err := store.FindBestEntity(charm.MustParseURL(test.url), test.channel, nil)
			if test.expectError != "" {
				c.Assert(err, gc.ErrorMatches, test.expectError)
				if test.expectErrorCause != nil {
					c.Assert(errgo.Cause(err), gc.Equals, test.expectErrorCause)
				}
				continue
			}
			c.Assert(err, gc.Equals, nil)
			c.Assert(EntityResolvedURL(entity), jc.DeepEquals, test.expectID)
		}
	}
}

var matchingInterfacesQueryTests = []struct {
	required []string
	provided []string
	expect   []string
}{{
	provided: []string{"a"},
	expect: []string{
		"cs:~charmers/" + storetesting.SearchSeries[1] + "/wordpress-1",
		"cs:~charmers/" + storetesting.SearchSeries[1] + "/wordpress-2",
	},
}, {
	provided: []string{"a", "b", "d"},
	required: []string{"b", "c", "e"},
	expect: []string{
		"cs:~charmers/" + storetesting.SearchSeries[1] + "/mysql-1",
		"cs:~charmers/" + storetesting.SearchSeries[1] + "/wordpress-1",
		"cs:~charmers/" + storetesting.SearchSeries[1] + "/wordpress-2",
	},
}, {
	required: []string{"x"},
	expect: []string{
		"cs:~charmers/" + storetesting.SearchSeries[1] + "/mysql-1",
		"cs:~charmers/" + storetesting.SearchSeries[1] + "/wordpress-2",
	},
}, {
	expect: []string{},
}}

func (s *StoreSuite) TestMatchingInterfacesQuery(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()
	entities := []*mongodoc.Entity{{
		URL:                     charm.MustParseURL("~charmers/" + storetesting.SearchSeries[1] + "/wordpress-1"),
		PromulgatedURL:          charm.MustParseURL(storetesting.SearchSeries[1] + "/wordpress-1"),
		CharmProvidedInterfaces: []string{"a", "b"},
		CharmRequiredInterfaces: []string{"b", "c"},
	}, {
		URL:                     charm.MustParseURL("~charmers/" + storetesting.SearchSeries[1] + "/wordpress-2"),
		PromulgatedURL:          charm.MustParseURL(storetesting.SearchSeries[1] + "/wordpress-2"),
		CharmProvidedInterfaces: []string{"a", "b"},
		CharmRequiredInterfaces: []string{"b", "c", "x"},
	}, {
		URL:                     charm.MustParseURL("~charmers/" + storetesting.SearchSeries[1] + "/mysql-1"),
		PromulgatedURL:          charm.MustParseURL(storetesting.SearchSeries[1] + "/mysql-1"),
		CharmProvidedInterfaces: []string{"d", "b"},
		CharmRequiredInterfaces: []string{"e", "x"},
	}}
	for _, e := range entities {
		err := store.DB.Entities().Insert(denormalizedEntity(e))
		c.Assert(err, gc.Equals, nil)
	}
	for i, test := range matchingInterfacesQueryTests {
		c.Logf("test %d: req %v; prov %v", i, test.required, test.provided)
		var entities []*mongodoc.Entity
		err := store.MatchingInterfacesQuery(test.required, test.provided).All(&entities)
		c.Assert(err, gc.Equals, nil)
		var got []string
		for _, e := range entities {
			got = append(got, e.URL.String())
		}
		sort.Strings(got)
		c.Assert(got, jc.DeepEquals, test.expect)
	}
}

var updateEntityTests = []struct {
	url       string
	expectErr string
}{{
	url: "~charmers/" + storetesting.SearchSeries[1] + "/wordpress-10",
}, {
	url:       "~charmers/" + storetesting.SearchSeries[0] + "/wordpress-10",
	expectErr: `cannot update "cs:` + storetesting.SearchSeries[0] + `/wordpress-10": not found`,
}}

func (s *StoreSuite) TestUpdateEntity(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()
	for i, test := range updateEntityTests {
		c.Logf("test %d. %s", i, test.url)
		url := router.MustNewResolvedURL(test.url, 10)
		_, err := store.DB.Entities().RemoveAll(nil)
		c.Assert(err, gc.Equals, nil)
		err = store.DB.Entities().Insert(denormalizedEntity(&mongodoc.Entity{
			URL:            charm.MustParseURL("~charmers/" + storetesting.SearchSeries[1] + "/wordpress-10"),
			PromulgatedURL: charm.MustParseURL(storetesting.SearchSeries[1] + "/wordpress-4"),
		}))
		c.Assert(err, gc.Equals, nil)
		err = store.UpdateEntity(url, bson.D{{"$set", bson.D{{"extrainfo.test", []byte("PASS")}}}})
		if test.expectErr != "" {
			c.Assert(err, gc.ErrorMatches, test.expectErr)
		} else {
			c.Assert(err, gc.Equals, nil)
			entity, err := store.FindEntity(url, nil)
			c.Assert(err, gc.Equals, nil)
			c.Assert(string(entity.ExtraInfo["test"]), gc.Equals, "PASS")
		}
	}
}

var updateBaseEntityTests = []struct {
	url       string
	expectErr string
}{{
	url: "~charmers/" + storetesting.SearchSeries[1] + "/wordpress-10",
}, {
	url:       "~charmers/" + storetesting.SearchSeries[0] + "/mysql-10",
	expectErr: `cannot update base entity for "cs:` + storetesting.SearchSeries[0] + `/mysql-10": not found`,
}}

func (s *StoreSuite) TestUpdateBaseEntity(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()
	for i, test := range updateBaseEntityTests {
		c.Logf("test %d. %s", i, test.url)
		url := router.MustNewResolvedURL(test.url, 10)
		_, err := store.DB.BaseEntities().RemoveAll(nil)
		c.Assert(err, gc.Equals, nil)
		err = store.DB.BaseEntities().Insert(&mongodoc.BaseEntity{
			URL:         charm.MustParseURL("~charmers/wordpress"),
			User:        "charmers",
			Name:        "wordpress",
			Promulgated: true,
		})
		c.Assert(err, gc.Equals, nil)
		err = store.UpdateBaseEntity(url, bson.D{{"$set", bson.D{{"channelacls.unpublished", mongodoc.ACL{
			Read: []string{"test"},
		}}}}})
		if test.expectErr != "" {
			c.Assert(err, gc.ErrorMatches, test.expectErr)
		} else {
			c.Assert(err, gc.Equals, nil)
			baseEntity, err := store.FindBaseEntity(&url.URL, nil)
			c.Assert(err, gc.Equals, nil)
			c.Assert(baseEntity.ChannelACLs[params.UnpublishedChannel].Read, jc.DeepEquals, []string{"test"})
		}
	}
}

var promulgateTests = []struct {
	about              string
	entities           []*mongodoc.Entity
	baseEntities       []*mongodoc.BaseEntity
	url                string
	promulgate         bool
	expectErr          string
	expectEntities     []*mongodoc.Entity
	expectBaseEntities []*mongodoc.BaseEntity
}{{
	about: "single charm not already promulgated",
	entities: []*mongodoc.Entity{
		entity("~charmers/"+storetesting.SearchSeries[1]+"/wordpress-0", ""),
	},
	baseEntities: []*mongodoc.BaseEntity{
		baseEntity("~charmers/wordpress", false),
	},
	url:        "~charmers/" + storetesting.SearchSeries[1] + "/wordpress-0",
	promulgate: true,
	expectEntities: []*mongodoc.Entity{
		entity("~charmers/"+storetesting.SearchSeries[1]+"/wordpress-0", storetesting.SearchSeries[1]+"/wordpress-0"),
	},
	expectBaseEntities: []*mongodoc.BaseEntity{
		baseEntity("~charmers/wordpress", true),
	},
}, {
	about: "multiple series not already promulgated",
	entities: []*mongodoc.Entity{
		entity("~charmers/"+storetesting.SearchSeries[1]+"/wordpress-0", ""),
		entity("~charmers/"+storetesting.SearchSeries[0]+"/wordpress-0", ""),
	},
	baseEntities: []*mongodoc.BaseEntity{
		baseEntity("~charmers/wordpress", false),
	},
	url:        "~charmers/" + storetesting.SearchSeries[1] + "/wordpress-0",
	promulgate: true,
	expectEntities: []*mongodoc.Entity{
		entity("~charmers/"+storetesting.SearchSeries[1]+"/wordpress-0", storetesting.SearchSeries[1]+"/wordpress-0"),
		entity("~charmers/"+storetesting.SearchSeries[0]+"/wordpress-0", storetesting.SearchSeries[0]+"/wordpress-0"),
	},
	expectBaseEntities: []*mongodoc.BaseEntity{
		baseEntity("~charmers/wordpress", true),
	},
}, {
	about: "charm promulgated as different user",
	entities: []*mongodoc.Entity{
		entity("~charmers/"+storetesting.SearchSeries[1]+"/wordpress-0", storetesting.SearchSeries[1]+"/wordpress-0"),
		entity("~test-charmers/"+storetesting.SearchSeries[1]+"/wordpress-0", ""),
	},
	baseEntities: []*mongodoc.BaseEntity{
		baseEntity("~charmers/wordpress", true),
		baseEntity("~test-charmers/wordpress", false),
	},
	url:        "~test-charmers/" + storetesting.SearchSeries[1] + "/wordpress-0",
	promulgate: true,
	expectEntities: []*mongodoc.Entity{
		entity("~charmers/"+storetesting.SearchSeries[1]+"/wordpress-0", storetesting.SearchSeries[1]+"/wordpress-0"),
		entity("~test-charmers/"+storetesting.SearchSeries[1]+"/wordpress-0", storetesting.SearchSeries[1]+"/wordpress-1"),
	},
	expectBaseEntities: []*mongodoc.BaseEntity{
		baseEntity("~charmers/wordpress", false),
		baseEntity("~test-charmers/wordpress", true),
	},
}, {
	about: "single charm already promulgated",
	entities: []*mongodoc.Entity{
		entity("~charmers/"+storetesting.SearchSeries[1]+"/wordpress-0", storetesting.SearchSeries[1]+"/wordpress-0"),
	},
	baseEntities: []*mongodoc.BaseEntity{
		baseEntity("~charmers/wordpress", true),
	},
	url:        "~charmers/" + storetesting.SearchSeries[1] + "/wordpress-0",
	promulgate: true,
	expectEntities: []*mongodoc.Entity{
		entity("~charmers/"+storetesting.SearchSeries[1]+"/wordpress-0", storetesting.SearchSeries[1]+"/wordpress-0"),
	},
	expectBaseEntities: []*mongodoc.BaseEntity{
		baseEntity("~charmers/wordpress", true),
	},
}, {
	about: "unrelated charms are unaffected",
	entities: []*mongodoc.Entity{
		entity("~charmers/"+storetesting.SearchSeries[1]+"/wordpress-0", ""),
		entity("~test-charmers/"+storetesting.SearchSeries[1]+"/mysql-0", storetesting.SearchSeries[1]+"/mysql-0"),
	},
	baseEntities: []*mongodoc.BaseEntity{
		baseEntity("~charmers/wordpress", false),
		baseEntity("~test-charmers/mysql", true),
	},
	url:        "~charmers/" + storetesting.SearchSeries[1] + "/wordpress-0",
	promulgate: true,
	expectEntities: []*mongodoc.Entity{
		entity("~charmers/"+storetesting.SearchSeries[1]+"/wordpress-0", storetesting.SearchSeries[1]+"/wordpress-0"),
		entity("~test-charmers/"+storetesting.SearchSeries[1]+"/mysql-0", storetesting.SearchSeries[1]+"/mysql-0"),
	},
	expectBaseEntities: []*mongodoc.BaseEntity{
		baseEntity("~charmers/wordpress", true),
		baseEntity("~test-charmers/mysql", true),
	},
}, {
	about: "only one owner promulgated",
	entities: []*mongodoc.Entity{
		entity("~charmers/"+storetesting.SearchSeries[1]+"/wordpress-0", ""),
		entity("~test-charmers/"+storetesting.SearchSeries[1]+"/wordpress-0", storetesting.SearchSeries[1]+"/wordpress-0"),
		entity("~test2-charmers/"+storetesting.SearchSeries[1]+"/wordpress-0", storetesting.SearchSeries[1]+"/wordpress-1"),
	},
	baseEntities: []*mongodoc.BaseEntity{
		baseEntity("~charmers/wordpress", false),
		baseEntity("~test-charmers/wordpress", false),
		baseEntity("~test2-charmers/wordpress", true),
	},
	url:        "~charmers/" + storetesting.SearchSeries[1] + "/wordpress-0",
	promulgate: true,
	expectEntities: []*mongodoc.Entity{
		entity("~charmers/"+storetesting.SearchSeries[1]+"/wordpress-0", storetesting.SearchSeries[1]+"/wordpress-2"),
		entity("~test-charmers/"+storetesting.SearchSeries[1]+"/wordpress-0", storetesting.SearchSeries[1]+"/wordpress-0"),
		entity("~test2-charmers/"+storetesting.SearchSeries[1]+"/wordpress-0", storetesting.SearchSeries[1]+"/wordpress-1"),
	},
	expectBaseEntities: []*mongodoc.BaseEntity{
		baseEntity("~charmers/wordpress", true),
		baseEntity("~test-charmers/wordpress", false),
		baseEntity("~test2-charmers/wordpress", false),
	},
}, {
	about: "recovers from two promulgated base entities",
	entities: []*mongodoc.Entity{
		entity("~charmers/"+storetesting.SearchSeries[1]+"/wordpress-0", ""),
		entity("~test-charmers/"+storetesting.SearchSeries[1]+"/wordpress-0", storetesting.SearchSeries[1]+"/wordpress-0"),
		entity("~test-charmers/"+storetesting.SearchSeries[1]+"/wordpress-1", storetesting.SearchSeries[1]+"/wordpress-2"),
		entity("~test2-charmers/"+storetesting.SearchSeries[1]+"/wordpress-0", storetesting.SearchSeries[1]+"/wordpress-1"),
	},
	baseEntities: []*mongodoc.BaseEntity{
		baseEntity("~charmers/wordpress", false),
		baseEntity("~test-charmers/wordpress", true),
		baseEntity("~test2-charmers/wordpress", true),
	},
	url:        "~test2-charmers/" + storetesting.SearchSeries[1] + "/wordpress-0",
	promulgate: true,
	expectEntities: []*mongodoc.Entity{
		entity("~charmers/"+storetesting.SearchSeries[1]+"/wordpress-0", ""),
		entity("~test-charmers/"+storetesting.SearchSeries[1]+"/wordpress-0", storetesting.SearchSeries[1]+"/wordpress-0"),
		entity("~test-charmers/"+storetesting.SearchSeries[1]+"/wordpress-1", storetesting.SearchSeries[1]+"/wordpress-2"),
		entity("~test2-charmers/"+storetesting.SearchSeries[1]+"/wordpress-0", storetesting.SearchSeries[1]+"/wordpress-1"),
	},
	expectBaseEntities: []*mongodoc.BaseEntity{
		baseEntity("~charmers/wordpress", false),
		baseEntity("~test-charmers/wordpress", false),
		baseEntity("~test2-charmers/wordpress", true),
	},
}, {
	about: "multiple series already promulgated",
	entities: []*mongodoc.Entity{
		entity("~charmers/"+storetesting.SearchSeries[1]+"/wordpress-0", storetesting.SearchSeries[1]+"/wordpress-2"),
		entity("~charmers/"+storetesting.SearchSeries[0]+"/wordpress-0", storetesting.SearchSeries[0]+"/wordpress-1"),
		entity("~test-charmers/"+storetesting.SearchSeries[1]+"/wordpress-0", ""),
		entity("~test-charmers/utopic/wordpress-0", ""),
	},
	baseEntities: []*mongodoc.BaseEntity{
		baseEntity("~charmers/wordpress", true),
		baseEntity("~test-charmers/wordpress", false),
	},
	url:        "~test-charmers/" + storetesting.SearchSeries[1] + "/wordpress-0",
	promulgate: true,
	expectEntities: []*mongodoc.Entity{
		entity("~charmers/"+storetesting.SearchSeries[1]+"/wordpress-0", storetesting.SearchSeries[1]+"/wordpress-2"),
		entity("~charmers/"+storetesting.SearchSeries[0]+"/wordpress-0", storetesting.SearchSeries[0]+"/wordpress-1"),
		entity("~test-charmers/"+storetesting.SearchSeries[1]+"/wordpress-0", storetesting.SearchSeries[1]+"/wordpress-3"),
		entity("~test-charmers/utopic/wordpress-0", "utopic/wordpress-0"),
	},
	expectBaseEntities: []*mongodoc.BaseEntity{
		baseEntity("~charmers/wordpress", false),
		baseEntity("~test-charmers/wordpress", true),
	},
}, {
	about: "multi-series with old single series",
	entities: []*mongodoc.Entity{
		entity("~charmers/"+storetesting.SearchSeries[1]+"/wordpress-0", storetesting.SearchSeries[1]+"/wordpress-2"),
		entity("~charmers/"+storetesting.SearchSeries[0]+"/wordpress-0", storetesting.SearchSeries[0]+"/wordpress-3"),
		entity("~test-charmers/wordpress-0", "", storetesting.SearchSeries[1], storetesting.SearchSeries[0], "xenial"),
	},
	baseEntities: []*mongodoc.BaseEntity{
		baseEntity("~charmers/wordpress", true),
		baseEntity("~test-charmers/wordpress", true),
	},
	url:        "~test-charmers/wordpress-0",
	promulgate: true,
	expectEntities: []*mongodoc.Entity{
		entity("~charmers/"+storetesting.SearchSeries[1]+"/wordpress-0", storetesting.SearchSeries[1]+"/wordpress-2"),
		entity("~charmers/"+storetesting.SearchSeries[0]+"/wordpress-0", storetesting.SearchSeries[0]+"/wordpress-3"),
		entity("~test-charmers/wordpress-0", "wordpress-4", storetesting.SearchSeries[1], storetesting.SearchSeries[0], "xenial"),
	},
	expectBaseEntities: []*mongodoc.BaseEntity{
		baseEntity("~charmers/wordpress", false),
		baseEntity("~test-charmers/wordpress", true),
	},
}, {
	about: "multi-series with old multi-series",
	entities: []*mongodoc.Entity{
		entity("~charmers/"+storetesting.SearchSeries[1]+"/wordpress-0", storetesting.SearchSeries[1]+"/wordpress-2"),
		entity("~charmers/"+storetesting.SearchSeries[0]+"/wordpress-0", storetesting.SearchSeries[0]+"/wordpress-3"),
		entity("~charmers/wordpress-0", "wordpress-3", storetesting.SearchSeries[1], "xenial"),
		entity("~test-charmers/"+storetesting.SearchSeries[1]+"/wordpress-0", ""),
		entity("~test-charmers/"+storetesting.SearchSeries[0]+"/wordpress-3", ""),
		entity("~test-charmers/wordpress-4", "", storetesting.SearchSeries[0], "xenial"),
	},
	baseEntities: []*mongodoc.BaseEntity{
		baseEntity("~charmers/wordpress", true),
		baseEntity("~test-charmers/wordpress", true),
	},
	url:        "~test-charmers/wordpress-0",
	promulgate: true,
	expectEntities: []*mongodoc.Entity{
		entity("~charmers/"+storetesting.SearchSeries[1]+"/wordpress-0", storetesting.SearchSeries[1]+"/wordpress-2"),
		entity("~charmers/"+storetesting.SearchSeries[0]+"/wordpress-0", storetesting.SearchSeries[0]+"/wordpress-3"),
		entity("~charmers/wordpress-0", "wordpress-3", storetesting.SearchSeries[1], "xenial"),
		entity("~test-charmers/"+storetesting.SearchSeries[1]+"/wordpress-0", ""),
		entity("~test-charmers/"+storetesting.SearchSeries[0]+"/wordpress-3", ""),
		entity("~test-charmers/wordpress-4", "wordpress-4", storetesting.SearchSeries[0], "xenial"),
	},
	expectBaseEntities: []*mongodoc.BaseEntity{
		baseEntity("~charmers/wordpress", false),
		baseEntity("~test-charmers/wordpress", true),
	},
}, {
	about: "multi-series with different sets of supported series",
	entities: []*mongodoc.Entity{
		entity("~test-charmers/wordpress-0", "", storetesting.SearchSeries[0], "xenial"),
		entity("~test-charmers/wordpress-1", "", storetesting.SearchSeries[1], "xenial"),
	},
	baseEntities: []*mongodoc.BaseEntity{
		baseEntity("~test-charmers/wordpress", false),
	},
	url:        "~test-charmers/wordpress-0",
	promulgate: true,
	expectEntities: []*mongodoc.Entity{
		entity("~test-charmers/wordpress-0", "", storetesting.SearchSeries[0], "xenial"),
		entity("~test-charmers/wordpress-1", "wordpress-0", storetesting.SearchSeries[1], "xenial"),
	},
	expectBaseEntities: []*mongodoc.BaseEntity{
		baseEntity("~test-charmers/wordpress", true),
	},
}, {
	// The single-series entity doesn't get promulgated because it's a single series
	// charm and we only promulgate single-series charms if the
	// new charm has no multi-series entities.
	about: "multi-series with different sets of supported series and previous multi-series",
	entities: []*mongodoc.Entity{
		entity("~charmers/"+storetesting.SearchSeries[0]+"/wordpress-5", "wordpress-5"),
		entity("~charmers/wordpress-0", "wordpress-6", storetesting.SearchSeries[0], "xenial"),
		entity("~charmers/wordpress-1", "wordpress-7", storetesting.SearchSeries[1], "xenial"),
		entity("~test-charmers/"+storetesting.SearchSeries[0]+"/wordpress-9", ""),
		entity("~test-charmers/wordpress-0", "", storetesting.SearchSeries[1]),
		entity("~test-charmers/wordpress-1", "", storetesting.SearchSeries[1], "xenial"),
	},
	baseEntities: []*mongodoc.BaseEntity{
		baseEntity("~charmers/wordpress", true),
		baseEntity("~test-charmers/wordpress", false),
	},
	url:        "~test-charmers/wordpress-1",
	promulgate: true,
	expectEntities: []*mongodoc.Entity{
		entity("~charmers/"+storetesting.SearchSeries[0]+"/wordpress-5", "wordpress-5"),
		entity("~charmers/wordpress-0", "wordpress-6", storetesting.SearchSeries[0], "xenial"),
		entity("~charmers/wordpress-1", "wordpress-7", storetesting.SearchSeries[1], "xenial"),
		entity("~test-charmers/"+storetesting.SearchSeries[0]+"/wordpress-9", ""),
		entity("~test-charmers/wordpress-0", "", storetesting.SearchSeries[1]),
		entity("~test-charmers/wordpress-1", "wordpress-8", storetesting.SearchSeries[1], "xenial"),
	},
	expectBaseEntities: []*mongodoc.BaseEntity{
		baseEntity("~charmers/wordpress", false),
		baseEntity("~test-charmers/wordpress", true),
	},
}, {
	// The non-multi-series charm should be left alone because
	// we never move from multi-series to non-multi-series.
	about: "old multi-series with new non-multi-series",
	entities: []*mongodoc.Entity{
		entity("~charmers/"+storetesting.SearchSeries[0]+"/wordpress-5", "wordpress-5"),
		entity("~charmers/wordpress-1", "wordpress-7", storetesting.SearchSeries[1], "xenial"),
		entity("~test-charmers/"+storetesting.SearchSeries[0]+"/wordpress-9", ""),
	},
	baseEntities: []*mongodoc.BaseEntity{
		baseEntity("~charmers/wordpress", true),
		baseEntity("~test-charmers/wordpress", false),
	},
	url:        "~test-charmers/" + storetesting.SearchSeries[0] + "/wordpress-9",
	promulgate: true,
	expectEntities: []*mongodoc.Entity{
		entity("~charmers/"+storetesting.SearchSeries[0]+"/wordpress-5", "wordpress-5"),
		entity("~charmers/wordpress-1", "wordpress-7", storetesting.SearchSeries[1], "xenial"),
		entity("~test-charmers/"+storetesting.SearchSeries[0]+"/wordpress-9", ""),
	},
	expectBaseEntities: []*mongodoc.BaseEntity{
		baseEntity("~charmers/wordpress", false),
		baseEntity("~test-charmers/wordpress", true),
	},
}, {
	about: "promulgate bundle",
	entities: []*mongodoc.Entity{
		entity("~charmers/bundle/wordpress-0", "bundle/wordpress-2"),
		entity("~charmers/bundle/wordpress-1", "bundle/wordpress-3"),
		entity("~test-charmers/bundle/wordpress-0", "bundle/wordpress-0"),
		entity("~test-charmers/bundle/wordpress-1", ""),
	},
	baseEntities: []*mongodoc.BaseEntity{
		baseEntity("~charmers/wordpress", true),
		baseEntity("~test-charmers/wordpress", false),
	},
	url:        "~test-charmers/bundle/wordpress-1",
	promulgate: true,
	expectEntities: []*mongodoc.Entity{
		entity("~charmers/bundle/wordpress-0", "bundle/wordpress-2"),
		entity("~charmers/bundle/wordpress-1", "bundle/wordpress-3"),
		entity("~test-charmers/bundle/wordpress-0", "bundle/wordpress-0"),
		entity("~test-charmers/bundle/wordpress-1", "bundle/wordpress-4"),
	},
	expectBaseEntities: []*mongodoc.BaseEntity{
		baseEntity("~charmers/wordpress", false),
		baseEntity("~test-charmers/wordpress", true),
	},
}, {
	about: "unpromulgate single promulgated charm ",
	entities: []*mongodoc.Entity{
		entity("~charmers/"+storetesting.SearchSeries[1]+"/wordpress-0", storetesting.SearchSeries[1]+"/wordpress-0"),
	},
	baseEntities: []*mongodoc.BaseEntity{
		baseEntity("~charmers/wordpress", true),
	},
	url:        "~charmers/" + storetesting.SearchSeries[1] + "/wordpress-0",
	promulgate: false,
	expectEntities: []*mongodoc.Entity{
		entity("~charmers/"+storetesting.SearchSeries[1]+"/wordpress-0", storetesting.SearchSeries[1]+"/wordpress-0"),
	},
	expectBaseEntities: []*mongodoc.BaseEntity{
		baseEntity("~charmers/wordpress", false),
	},
}, {
	about: "unpromulgate single unpromulgated charm ",
	entities: []*mongodoc.Entity{
		entity("~charmers/"+storetesting.SearchSeries[1]+"/wordpress-0", ""),
	},
	baseEntities: []*mongodoc.BaseEntity{
		baseEntity("~charmers/wordpress", false),
	},
	url:        "~charmers/" + storetesting.SearchSeries[1] + "/wordpress-0",
	promulgate: false,
	expectEntities: []*mongodoc.Entity{
		entity("~charmers/"+storetesting.SearchSeries[1]+"/wordpress-0", ""),
	},
	expectBaseEntities: []*mongodoc.BaseEntity{
		baseEntity("~charmers/wordpress", false),
	},
}}

func (s *StoreSuite) TestSetPromulgated(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()
	for i, test := range promulgateTests {
		c.Logf("\ntest %d. %s", i, test.about)
		url := router.MustNewResolvedURL(test.url, -1)
		_, err := store.DB.Entities().RemoveAll(nil)
		c.Assert(err, gc.Equals, nil)
		_, err = store.DB.BaseEntities().RemoveAll(nil)
		c.Assert(err, gc.Equals, nil)
		for _, entity := range test.entities {
			err := store.DB.Entities().Insert(entity)
			c.Assert(err, gc.Equals, nil)
		}
		for _, baseEntity := range test.baseEntities {
			err := store.DB.BaseEntities().Insert(baseEntity)
			c.Assert(err, gc.Equals, nil)
		}
		err = store.SetPromulgated(url, test.promulgate)
		if test.expectErr != "" {
			c.Assert(err, gc.ErrorMatches, test.expectErr)
			continue
		}
		c.Assert(err, gc.Equals, nil)
		n, err := store.DB.Entities().Count()
		c.Assert(err, gc.Equals, nil)
		c.Assert(n, gc.Equals, len(test.expectEntities))
		n, err = store.DB.BaseEntities().Count()
		c.Assert(err, gc.Equals, nil)
		c.Assert(n, gc.Equals, len(test.expectBaseEntities))
		for i, expectEntity := range test.expectEntities {
			entity, err := store.FindEntity(EntityResolvedURL(expectEntity), nil)
			c.Assert(err, gc.Equals, nil)
			c.Assert(entity, jc.DeepEquals, expectEntity, gc.Commentf("entity %d", i))
		}
		for _, expectBaseEntity := range test.expectBaseEntities {
			baseEntity, err := store.FindBaseEntity(expectBaseEntity.URL, nil)
			c.Assert(err, gc.Equals, nil)
			c.Assert(storetesting.NormalizeBaseEntity(baseEntity), jc.DeepEquals, storetesting.NormalizeBaseEntity(expectBaseEntity))
		}
	}
}

func (s *StoreSuite) TestSetPromulgatedUpdateSearch(c *gc.C) {
	store := s.newStore(c, true)
	defer store.Close()

	wordpress := storetesting.NewCharm(&charm.Meta{
		Name: "wordpress",
	})
	addCharmForSearch(
		c,
		store,
		router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[1]+"/wordpress-0", 2),
		wordpress,
		nil,
		0,
	)
	addCharmForSearch(
		c,
		store,
		router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[0]+"/wordpress-0", 1),
		wordpress,
		nil,
		0,
	)
	addCharmForSearch(
		c,
		store,
		router.MustNewResolvedURL("~openstack-charmers/"+storetesting.SearchSeries[1]+"/wordpress-0", -1),
		wordpress,
		nil,
		0,
	)
	addCharmForSearch(
		c,
		store,
		router.MustNewResolvedURL("~openstack-charmers/"+storetesting.SearchSeries[0]+"/wordpress-0", -1),
		wordpress,
		nil,
		0,
	)
	url := router.MustNewResolvedURL("~openstack-charmers/"+storetesting.SearchSeries[1]+"/wordpress-0", -1)

	// Change the promulgated wordpress version to openstack-charmers.
	err := store.SetPromulgated(url, true)
	c.Assert(err, gc.Equals, nil)
	err = store.ES.RefreshIndex(s.TestIndex)
	c.Assert(err, gc.Equals, nil)
	// Check that the search records contain the correct information.
	var zdoc SearchDoc
	doc := zdoc
	err = store.ES.GetDocument(s.TestIndex, typeName, store.ES.getID(charm.MustParseURL("~charmers/"+storetesting.SearchSeries[1]+"/wordpress-0")), &doc)
	c.Assert(err, gc.Equals, nil)
	c.Assert(doc.PromulgatedURL, gc.IsNil)
	c.Assert(doc.PromulgatedRevision, gc.Equals, -1)
	doc = zdoc
	err = store.ES.GetDocument(s.TestIndex, typeName, store.ES.getID(charm.MustParseURL("~charmers/"+storetesting.SearchSeries[0]+"/wordpress-0")), &doc)
	c.Assert(err, gc.Equals, nil)
	c.Assert(doc.PromulgatedURL, gc.IsNil)
	c.Assert(doc.PromulgatedRevision, gc.Equals, -1)
	doc = zdoc
	err = store.ES.GetDocument(s.TestIndex, typeName, store.ES.getID(charm.MustParseURL("~openstack-charmers/"+storetesting.SearchSeries[1]+"/wordpress-0")), &doc)
	c.Assert(err, gc.Equals, nil)
	c.Assert(doc.PromulgatedURL.String(), gc.Equals, "cs:"+storetesting.SearchSeries[1]+"/wordpress-3")
	c.Assert(doc.PromulgatedRevision, gc.Equals, 3)
	doc = zdoc
	err = store.ES.GetDocument(s.TestIndex, typeName, store.ES.getID(charm.MustParseURL("~openstack-charmers/"+storetesting.SearchSeries[0]+"/wordpress-0")), &doc)
	c.Assert(err, gc.Equals, nil)
	c.Assert(doc.PromulgatedURL.String(), gc.Equals, "cs:"+storetesting.SearchSeries[0]+"/wordpress-2")
	c.Assert(doc.PromulgatedRevision, gc.Equals, 2)

	// Remove the promulgated flag from openstack-charmers, meaning wordpress is
	// no longer promulgated.
	err = store.SetPromulgated(url, false)
	c.Assert(err, gc.Equals, nil)
	err = store.ES.RefreshIndex(s.TestIndex)
	c.Assert(err, gc.Equals, nil)
	// Check that the search records contain the correct information.
	doc = zdoc
	err = store.ES.GetDocument(s.TestIndex, typeName, store.ES.getID(charm.MustParseURL("~charmers/"+storetesting.SearchSeries[1]+"/wordpress-0")), &doc)
	c.Assert(err, gc.Equals, nil)
	c.Assert(doc.PromulgatedURL, gc.IsNil)
	c.Assert(doc.PromulgatedRevision, gc.Equals, -1)
	doc = zdoc
	err = store.ES.GetDocument(s.TestIndex, typeName, store.ES.getID(charm.MustParseURL("~charmers/"+storetesting.SearchSeries[0]+"/wordpress-0")), &doc)
	c.Assert(err, gc.Equals, nil)
	c.Assert(doc.PromulgatedURL, gc.IsNil)
	c.Assert(doc.PromulgatedRevision, gc.Equals, -1)
	doc = zdoc
	err = store.ES.GetDocument(s.TestIndex, typeName, store.ES.getID(charm.MustParseURL("~openstack-charmers/"+storetesting.SearchSeries[1]+"/wordpress-0")), &doc)
	c.Assert(err, gc.Equals, nil)
	c.Assert(doc.PromulgatedURL, gc.IsNil)
	c.Assert(doc.PromulgatedRevision, gc.Equals, -1)
	doc = zdoc
	err = store.ES.GetDocument(s.TestIndex, typeName, store.ES.getID(charm.MustParseURL("~openstack-charmers/"+storetesting.SearchSeries[0]+"/wordpress-0")), &doc)
	c.Assert(err, gc.Equals, nil)
	c.Assert(doc.PromulgatedURL, gc.IsNil)
	c.Assert(doc.PromulgatedRevision, gc.Equals, -1)
}

var entityResolvedURLTests = []struct {
	about  string
	entity *mongodoc.Entity
	rurl   *router.ResolvedURL
}{{
	about: "user owned, published",
	entity: &mongodoc.Entity{
		URL: charm.MustParseURL("~charmers/" + storetesting.SearchSeries[0] + "/wordpress-23"),
	},
	rurl: &router.ResolvedURL{
		URL:                 *charm.MustParseURL("~charmers/" + storetesting.SearchSeries[0] + "/wordpress-23"),
		PromulgatedRevision: -1,
	},
}, {
	about: "promulgated, published",
	entity: &mongodoc.Entity{
		URL:            charm.MustParseURL("~charmers/" + storetesting.SearchSeries[0] + "/wordpress-23"),
		PromulgatedURL: charm.MustParseURL(storetesting.SearchSeries[0] + "/wordpress-4"),
	},
	rurl: &router.ResolvedURL{
		URL:                 *charm.MustParseURL("~charmers/" + storetesting.SearchSeries[0] + "/wordpress-23"),
		PromulgatedRevision: 4,
	},
}}

func (s *StoreSuite) TestEntityResolvedURL(c *gc.C) {
	for i, test := range entityResolvedURLTests {
		c.Logf("test %d: %s", i, test.about)
		c.Assert(EntityResolvedURL(test.entity), gc.DeepEquals, test.rurl)
	}
}

func (s *StoreSuite) TestCopyCopiesSessions(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()

	wordpress := storetesting.Charms.CharmDir("wordpress")
	url := MustParseResolvedURL("23 cs:~charmers/" + storetesting.SearchSeries[0] + "/wordpress-23")
	err := store.AddCharmWithArchive(url, wordpress)
	c.Assert(err, gc.Equals, nil)

	store1 := store.Copy()
	defer store1.Close()

	// Close the store we copied from. The copy should be unaffected.
	store.Close()

	entity, err := store1.FindEntity(url, nil)
	c.Assert(err, gc.Equals, nil)

	// Also check the blob store, as it has its own session reference.
	r, _, err := store1.BlobStore.Open(entity.BlobHash, nil)
	c.Assert(err, gc.Equals, nil)
	r.Close()

	// Also check the macaroon storage as that also has its own session reference.
	m, err := store1.Bakery.NewMacaroon(nil)
	c.Assert(err, gc.Equals, nil)
	c.Assert(m, gc.NotNil)
}

func (s *StoreSuite) TestAddAudit(c *gc.C) {
	filename := filepath.Join(c.MkDir(), "audit.log")
	config := ServerParams{
		AuditLogger: &lumberjack.Logger{
			Filename: filename,
		},
	}

	p, err := NewPool(s.Session.DB("juju_test"), nil, nil, config)
	c.Assert(err, gc.Equals, nil)
	defer p.Close()

	store := p.Store()
	defer store.Close()

	entries := []audit.Entry{{
		User:   "George Clooney",
		Op:     audit.OpSetPerm,
		Entity: charm.MustParseURL("cs:mycharm"),
		ACL: &audit.ACL{
			Read:  []string{"eleven", "ocean"},
			Write: []string{"brad", "pitt"},
		},
	}, {
		User: "Julia Roberts",
		Op:   audit.OpSetPerm,
	}}

	now := time.Now()
	for _, e := range entries {
		store.addAuditAtTime(e, now)
	}
	data, err := ioutil.ReadFile(filename)
	c.Assert(err, gc.Equals, nil)

	lines := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
	c.Assert(lines, gc.HasLen, len(entries))
	for i, e := range entries {
		e.Time = now
		c.Assert(lines[i], jc.JSONEquals, e)
	}
}

func (s *StoreSuite) TestAddAuditWithNoLumberjack(c *gc.C) {
	p, err := NewPool(s.Session.DB("juju_test"), nil, nil, ServerParams{})
	c.Assert(err, gc.Equals, nil)
	defer p.Close()

	store := p.Store()
	defer store.Close()

	// Check that it does not panic.
	store.AddAudit(audit.Entry{
		User:   "George Clooney",
		Op:     audit.OpSetPerm,
		Entity: charm.MustParseURL("cs:mycharm"),
		ACL: &audit.ACL{
			Read:  []string{"eleven", "ocean"},
			Write: []string{"brad", "pitt"},
		},
	})
}

func (s *StoreSuite) TestDenormalizeEntity(c *gc.C) {
	e := &mongodoc.Entity{
		URL: charm.MustParseURL("~someone/utopic/acharm-45"),
	}
	denormalizeEntity(e)
	c.Assert(e, jc.DeepEquals, &mongodoc.Entity{
		URL:                 charm.MustParseURL("~someone/utopic/acharm-45"),
		BaseURL:             charm.MustParseURL("~someone/acharm"),
		User:                "someone",
		Name:                "acharm",
		Revision:            45,
		Series:              "utopic",
		PromulgatedRevision: -1,
		SupportedSeries:     []string{"utopic"},
	})
}

func (s *StoreSuite) TestDenormalizePromulgatedEntity(c *gc.C) {
	e := &mongodoc.Entity{
		URL:            charm.MustParseURL("~someone/utopic/acharm-45"),
		PromulgatedURL: charm.MustParseURL("utopic/acharm-5"),
	}
	denormalizeEntity(e)
	c.Assert(e, jc.DeepEquals, &mongodoc.Entity{
		URL:                 charm.MustParseURL("~someone/utopic/acharm-45"),
		BaseURL:             charm.MustParseURL("~someone/acharm"),
		User:                "someone",
		Name:                "acharm",
		Revision:            45,
		Series:              "utopic",
		PromulgatedURL:      charm.MustParseURL("utopic/acharm-5"),
		PromulgatedRevision: 5,
		SupportedSeries:     []string{"utopic"},
	})
}

func (s *StoreSuite) TestDenormalizeBundleEntity(c *gc.C) {
	e := &mongodoc.Entity{
		URL: charm.MustParseURL("~someone/bundle/acharm-45"),
	}
	denormalizeEntity(e)
	c.Assert(e, jc.DeepEquals, &mongodoc.Entity{
		URL:                 charm.MustParseURL("~someone/bundle/acharm-45"),
		BaseURL:             charm.MustParseURL("~someone/acharm"),
		User:                "someone",
		Name:                "acharm",
		Revision:            45,
		Series:              "bundle",
		PromulgatedRevision: -1,
	})
}

func (s *StoreSuite) TestBundleCharms(c *gc.C) {
	// Populate the store with some testing charms.
	mysql := storetesting.Charms.CharmArchive(c.MkDir(), "mysql")
	store := s.newStore(c, true)
	defer store.Close()
	rurl := router.MustNewResolvedURL("cs:~charmers/saucy/mysql-0", 0)
	err := store.AddCharmWithArchive(rurl, mysql)
	c.Assert(err, gc.Equals, nil)
	err = store.Publish(rurl, nil, params.StableChannel)
	c.Assert(err, gc.Equals, nil)
	riak := storetesting.Charms.CharmArchive(c.MkDir(), "riak")
	rurl = router.MustNewResolvedURL("cs:~charmers/"+storetesting.SearchSeries[1]+"/riak-42", 42)
	err = store.AddCharmWithArchive(rurl, riak)
	c.Assert(err, gc.Equals, nil)
	err = store.Publish(rurl, nil, params.StableChannel)
	c.Assert(err, gc.Equals, nil)
	wordpress := storetesting.Charms.CharmArchive(c.MkDir(), "wordpress")
	rurl = router.MustNewResolvedURL("cs:~charmers/utopic/wordpress-47", 47)
	err = store.AddCharmWithArchive(rurl, wordpress)
	c.Assert(err, gc.Equals, nil)
	err = store.Publish(rurl, nil, params.StableChannel)
	c.Assert(err, gc.Equals, nil)
	varnish := storetesting.Charms.CharmArchive(c.MkDir(), "varnish")
	rurl = router.MustNewResolvedURL("cs:~charmers/trusty/varnish-2", 2)
	err = store.AddCharmWithArchive(rurl, varnish)
	c.Assert(err, gc.Equals, nil)
	err = store.Publish(rurl, nil, params.EdgeChannel)
	c.Assert(err, gc.Equals, nil)

	tests := []struct {
		about  string
		reqs   []requiredCharm
		charms map[string]charm.Charm
	}{{
		about: "no ids",
	}, {
		about: "fully qualified ids",
		reqs: []requiredCharm{
			{charm: "cs:~charmers/saucy/mysql-0"},
			{charm: "cs:~charmers/" + storetesting.SearchSeries[1] + "/riak-42"},
			{charm: "cs:~charmers/utopic/wordpress-47"},
		},
		charms: map[string]charm.Charm{
			"cs:~charmers/saucy/mysql-0":                                mysql,
			"cs:~charmers/" + storetesting.SearchSeries[1] + "/riak-42": riak,
			"cs:~charmers/utopic/wordpress-47":                          wordpress,
		},
	}, {
		about: "partial ids",
		reqs: []requiredCharm{
			{charm: "~charmers/utopic/wordpress"},
			{charm: "~charmers/riak"},
		},
		charms: map[string]charm.Charm{
			"~charmers/riak":             riak,
			"~charmers/utopic/wordpress": wordpress,
		},
	}, {
		about: "charm not found",
		reqs: []requiredCharm{
			{charm: "utopic/no-such"},
			{charm: "~charmers/mysql"},
		},
		charms: map[string]charm.Charm{
			"~charmers/mysql": mysql,
		},
	}, {
		about: "no charms found",
		reqs: []requiredCharm{
			{charm: "cs:~charmers/saucy/mysql-99"},                               // Revision not present.
			{charm: "cs:~charmers/" + storetesting.SearchSeries[0] + "/riak-42"}, // Series not present.
			{charm: "cs:~charmers/utopic/django-47"},                             // Name not present.
		},
	}, {
		about: "repeated charms",
		reqs: []requiredCharm{
			{charm: "cs:~charmers/saucy/mysql"},
			{charm: "cs:~charmers/" + storetesting.SearchSeries[1] + "/riak-42"},
			{charm: "~charmers/mysql"},
		},
		charms: map[string]charm.Charm{
			"cs:~charmers/saucy/mysql":                                  mysql,
			"cs:~charmers/" + storetesting.SearchSeries[1] + "/riak-42": riak,
			"~charmers/mysql":                                           mysql,
		},
	}, {
		about: "charms with a preferred channel",
		reqs: []requiredCharm{
			{charm: "~charmers/varnish", channel: "edge"},
		},
		charms: map[string]charm.Charm{
			"~charmers/varnish": varnish,
		},
	}}

	// Run the tests.
	for i, test := range tests {
		c.Logf("test %d: %s", i, test.about)
		charms, err := store.bundleCharms(test.reqs)
		c.Assert(err, gc.Equals, nil)
		// Ensure the charms returned are what we expect.
		c.Assert(charms, gc.HasLen, len(test.charms))
		for i, ch := range charms {
			expectCharm := test.charms[i]
			c.Assert(ch.Meta(), jc.DeepEquals, expectCharm.Meta())
			c.Assert(ch.Config(), jc.DeepEquals, expectCharm.Config())
			c.Assert(ch.Actions(), jc.DeepEquals, expectCharm.Actions())
			// Since the charm archive and the charm entity have a slightly
			// different concept of what a revision is, and since the revision
			// is not used for bundle validation, we can safely avoid checking
			// the charm revision.
		}
	}
}

func (s *StoreSuite) TestNewRevisionFirstTime(c *gc.C) {
	store := s.newStore(c, true)
	defer store.Close()
	id := charm.MustParseURL("~bob/" + storetesting.SearchSeries[0] + "/wordpress")
	rev, err := store.NewRevision(id)
	c.Assert(err, gc.Equals, nil)
	c.Assert(rev, gc.Equals, 0)
}

func (s *StoreSuite) TestNewRevisionSeveralTimes(c *gc.C) {
	store := s.newStore(c, true)
	defer store.Close()
	id := charm.MustParseURL("~bob/" + storetesting.SearchSeries[0] + "/wordpress")
	for i := 0; i < 5; i++ {
		rev, err := store.NewRevision(id)
		c.Assert(err, gc.Equals, nil)
		c.Assert(rev, gc.Equals, i)
	}
}

func (s *StoreSuite) TestNewRevisionConcurrent(c *gc.C) {
	store := s.newStore(c, true)
	defer store.Close()
	rch := make(chan string)
	expect := make(map[string]bool)
	for n := 0; n < 10; n++ {
		for i := 0; i < 3; i++ {
			id := charm.MustParseURL(fmt.Sprintf("c%d", n))
			expect[id.WithRevision(i).String()] = true
			go func() {
				rev, err := store.NewRevision(id)
				c.Check(err, gc.Equals, nil)
				id.Revision = rev
				rch <- id.String()
			}()
		}
	}
	got := make(map[string]bool)
	for i := 0; i < 10*3; i++ {
		got[<-rch] = true
	}
	c.Assert(got, jc.DeepEquals, expect)
}

func (s *StoreSuite) TestNewRevisionWithExistingSingleSeries(c *gc.C) {
	store := s.newStore(c, true)
	defer store.Close()
	for _, id := range MustParseResolvedURLs([]string{
		"2 ~charmers/" + storetesting.SearchSeries[0] + "/wordpress-11",
		"~charmers/" + storetesting.SearchSeries[0] + "/wordpress-12",
		"1 ~charmers/quantal/wordpress-14",
		"0 ~charmers/quantal/wordpress-5",
		"~bob/wordpress-20",
	}) {
		err := store.AddRevision(id)
		c.Assert(err, gc.Equals, nil)
	}
	rev, err := store.NewRevision(charm.MustParseURL("cs:~charmers/wordpress"))
	c.Assert(err, gc.Equals, nil)
	c.Assert(rev, gc.Equals, 15)

	rev, err = store.NewRevision(charm.MustParseURL("cs:wordpress"))
	c.Assert(err, gc.Equals, nil)
	c.Assert(rev, gc.Equals, 3)
}

func (s *StoreSuite) TestAddRevisionFirstTime(c *gc.C) {
	store := s.newStore(c, true)
	defer store.Close()
	id := router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[0]+"/wordpress-12", 10)
	err := store.AddRevision(id)
	c.Assert(err, gc.Equals, nil)

	rev, err := store.NewRevision(&id.URL)
	c.Assert(err, gc.Equals, nil)
	c.Assert(rev, gc.Equals, 13)

	rev, err = store.NewRevision(id.PromulgatedURL())
	c.Assert(err, gc.Equals, nil)
	c.Assert(rev, gc.Equals, 11)
}

func (s *StoreSuite) TestAddRevisionWithoutPromulgatedURL(c *gc.C) {
	store := s.newStore(c, true)
	defer store.Close()
	id := router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[0]+"/wordpress-12", -1)
	err := store.AddRevision(id)
	c.Assert(err, gc.Equals, nil)

	rev, err := store.NewRevision(&id.URL)
	c.Assert(err, gc.Equals, nil)
	c.Assert(rev, gc.Equals, 13)

	rev, err = store.NewRevision(charm.MustParseURL("~" + storetesting.SearchSeries[0] + "/wordpress"))
	c.Assert(err, gc.Equals, nil)
	c.Assert(rev, gc.Equals, 0)
}

func (s *StoreSuite) TestAddRevisionWithExisting(c *gc.C) {
	store := s.newStore(c, true)
	defer store.Close()
	id := router.MustNewResolvedURL("~bob/"+storetesting.SearchSeries[0]+"/wordpress-3", -1)
	err := store.AddRevision(id)
	c.Assert(err, gc.Equals, nil)

	rev, err := store.NewRevision(&id.URL)
	c.Assert(err, gc.Equals, nil)
	c.Assert(rev, gc.Equals, 4)

	id = router.MustNewResolvedURL("~bob/"+storetesting.SearchSeries[0]+"/wordpress-4", -1)
	err = store.AddRevision(id)
	c.Assert(err, gc.Equals, nil)

	rev, err = store.NewRevision(&id.URL)
	c.Assert(err, gc.Equals, nil)
	c.Assert(rev, gc.Equals, 5)
}

var publishTests = []struct {
	about              string
	url                *router.ResolvedURL
	channels           []params.Channel
	initialEntity      *mongodoc.Entity
	initialBaseEntity  *mongodoc.BaseEntity
	expectedEntity     *mongodoc.Entity
	expectedBaseEntity *mongodoc.BaseEntity
	expectedErr        string
}{{
	about:    "unpublished, single series, publish edge",
	url:      MustParseResolvedURL("~who/" + storetesting.SearchSeries[1] + "/django-42"),
	channels: []params.Channel{params.EdgeChannel},
	initialEntity: &mongodoc.Entity{
		URL: charm.MustParseURL("~who/" + storetesting.SearchSeries[1] + "/django-42"),
	},
	initialBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
	},
	expectedEntity: &mongodoc.Entity{
		URL: charm.MustParseURL("~who/" + storetesting.SearchSeries[1] + "/django-42"),
		Published: map[params.Channel]bool{
			params.EdgeChannel: true,
		},
	},
	expectedBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		ChannelEntities: map[params.Channel]map[string]*charm.URL{
			params.EdgeChannel: {
				storetesting.SearchSeries[1]: charm.MustParseURL("~who/" + storetesting.SearchSeries[1] + "/django-42"),
			},
		},
	},
}, {
	about:    "edge, single series, publish edge",
	url:      MustParseResolvedURL("~who/" + storetesting.SearchSeries[1] + "/django-42"),
	channels: []params.Channel{params.EdgeChannel},
	initialEntity: &mongodoc.Entity{
		URL: charm.MustParseURL("~who/" + storetesting.SearchSeries[1] + "/django-42"),
		Published: map[params.Channel]bool{
			params.EdgeChannel: true,
		},
	},
	initialBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		ChannelEntities: map[params.Channel]map[string]*charm.URL{
			params.EdgeChannel: {
				storetesting.SearchSeries[1]: charm.MustParseURL("~who/" + storetesting.SearchSeries[1] + "/django-41"),
			},
		},
	},
	expectedEntity: &mongodoc.Entity{
		URL: charm.MustParseURL("~who/" + storetesting.SearchSeries[1] + "/django-42"),
		Published: map[params.Channel]bool{
			params.EdgeChannel: true,
		},
	},
	expectedBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		ChannelEntities: map[params.Channel]map[string]*charm.URL{
			params.EdgeChannel: {
				storetesting.SearchSeries[1]: charm.MustParseURL("~who/" + storetesting.SearchSeries[1] + "/django-42"),
			},
		},
	},
}, {
	about:    "stable, single series, publish edge",
	url:      MustParseResolvedURL("~who/" + storetesting.SearchSeries[1] + "/django-42"),
	channels: []params.Channel{params.EdgeChannel},
	initialEntity: &mongodoc.Entity{
		URL: charm.MustParseURL("~who/" + storetesting.SearchSeries[1] + "/django-42"),
		Published: map[params.Channel]bool{
			params.StableChannel: true,
		},
	},
	initialBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		ChannelEntities: map[params.Channel]map[string]*charm.URL{
			params.StableChannel: {
				storetesting.SearchSeries[1]: charm.MustParseURL("~who/" + storetesting.SearchSeries[1] + "/django-42"),
			},
		},
	},
	expectedEntity: &mongodoc.Entity{
		URL: charm.MustParseURL("~who/" + storetesting.SearchSeries[1] + "/django-42"),
		Published: map[params.Channel]bool{
			params.EdgeChannel:   true,
			params.StableChannel: true,
		},
	},
	expectedBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		ChannelEntities: map[params.Channel]map[string]*charm.URL{
			params.StableChannel: {
				storetesting.SearchSeries[1]: charm.MustParseURL("~who/" + storetesting.SearchSeries[1] + "/django-42"),
			},
			params.EdgeChannel: {
				storetesting.SearchSeries[1]: charm.MustParseURL("~who/" + storetesting.SearchSeries[1] + "/django-42"),
			},
		},
	},
}, {
	about:    "unpublished, single series, publish stable",
	url:      MustParseResolvedURL("~who/" + storetesting.SearchSeries[1] + "/django-42"),
	channels: []params.Channel{params.StableChannel},
	initialEntity: &mongodoc.Entity{
		URL: charm.MustParseURL("~who/" + storetesting.SearchSeries[1] + "/django-42"),
	},
	initialBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
	},
	expectedEntity: &mongodoc.Entity{
		URL: charm.MustParseURL("~who/" + storetesting.SearchSeries[1] + "/django-42"),
		Published: map[params.Channel]bool{
			params.StableChannel: true,
		},
	},
	expectedBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		ChannelEntities: map[params.Channel]map[string]*charm.URL{
			params.StableChannel: {
				storetesting.SearchSeries[1]: charm.MustParseURL("~who/" + storetesting.SearchSeries[1] + "/django-42"),
			},
		},
	},
}, {
	about:    "edge, single series, publish stable",
	url:      MustParseResolvedURL("~who/" + storetesting.SearchSeries[1] + "/django-42"),
	channels: []params.Channel{params.StableChannel},
	initialEntity: &mongodoc.Entity{
		URL: charm.MustParseURL("~who/" + storetesting.SearchSeries[1] + "/django-42"),
		Published: map[params.Channel]bool{
			params.EdgeChannel: true,
		},
	},
	initialBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		ChannelEntities: map[params.Channel]map[string]*charm.URL{
			params.EdgeChannel: {
				storetesting.SearchSeries[1]: charm.MustParseURL("~who/" + storetesting.SearchSeries[1] + "/django-41"),
			},
		},
	},
	expectedEntity: &mongodoc.Entity{
		URL: charm.MustParseURL("~who/" + storetesting.SearchSeries[1] + "/django-42"),
		Published: map[params.Channel]bool{
			params.EdgeChannel:   true,
			params.StableChannel: true,
		},
	},
	expectedBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		ChannelEntities: map[params.Channel]map[string]*charm.URL{
			params.EdgeChannel: {
				storetesting.SearchSeries[1]: charm.MustParseURL("~who/" + storetesting.SearchSeries[1] + "/django-41"),
			},
			params.StableChannel: {
				storetesting.SearchSeries[1]: charm.MustParseURL("~who/" + storetesting.SearchSeries[1] + "/django-42"),
			},
		},
	},
}, {
	about:    "stable, single series, publish stable",
	url:      MustParseResolvedURL("~who/" + storetesting.SearchSeries[1] + "/django-42"),
	channels: []params.Channel{params.StableChannel},
	initialEntity: &mongodoc.Entity{
		URL: charm.MustParseURL("~who/" + storetesting.SearchSeries[1] + "/django-42"),
		Published: map[params.Channel]bool{
			params.StableChannel: true,
		},
	},
	initialBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		ChannelEntities: map[params.Channel]map[string]*charm.URL{
			params.StableChannel: {
				storetesting.SearchSeries[1]: charm.MustParseURL("~who/" + storetesting.SearchSeries[1] + "/django-40"),
			},
		},
	},
	expectedEntity: &mongodoc.Entity{
		URL: charm.MustParseURL("~who/" + storetesting.SearchSeries[1] + "/django-42"),
		Published: map[params.Channel]bool{
			params.StableChannel: true,
		},
	},
	expectedBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		ChannelEntities: map[params.Channel]map[string]*charm.URL{
			params.StableChannel: {
				storetesting.SearchSeries[1]: charm.MustParseURL("~who/" + storetesting.SearchSeries[1] + "/django-42"),
			},
		},
	},
}, {
	about:    "unpublished, multi series, publish edge",
	url:      MustParseResolvedURL("~who/django-42"),
	channels: []params.Channel{params.EdgeChannel},
	initialEntity: &mongodoc.Entity{
		URL:             charm.MustParseURL("~who/django-42"),
		SupportedSeries: []string{storetesting.SearchSeries[1], "wily"},
	},
	initialBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
	},
	expectedEntity: &mongodoc.Entity{
		URL:             charm.MustParseURL("~who/django-42"),
		SupportedSeries: []string{storetesting.SearchSeries[1], "wily"},
		Published: map[params.Channel]bool{
			params.EdgeChannel: true,
		},
	},
	expectedBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		ChannelEntities: map[params.Channel]map[string]*charm.URL{
			params.EdgeChannel: {
				storetesting.SearchSeries[1]: charm.MustParseURL("~who/django-42"),
				"wily":                       charm.MustParseURL("~who/django-42"),
			},
		},
	},
}, {
	about:    "edge, multi series, publish edge",
	url:      MustParseResolvedURL("~who/django-42"),
	channels: []params.Channel{params.EdgeChannel},
	initialEntity: &mongodoc.Entity{
		URL: charm.MustParseURL("~who/django-42"),
		Published: map[params.Channel]bool{
			params.EdgeChannel: true,
		},
		SupportedSeries: []string{storetesting.SearchSeries[1], "wily"},
	},
	initialBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		ChannelEntities: map[params.Channel]map[string]*charm.URL{
			params.EdgeChannel: {
				storetesting.SearchSeries[0]: charm.MustParseURL("~who/django-0"),
				storetesting.SearchSeries[1]: charm.MustParseURL("~who/" + storetesting.SearchSeries[1] + "/django-0"),
			},
		},
	},
	expectedEntity: &mongodoc.Entity{
		URL: charm.MustParseURL("~who/django-42"),
		Published: map[params.Channel]bool{
			params.EdgeChannel: true,
		},
		SupportedSeries: []string{storetesting.SearchSeries[1], "wily"},
	},
	expectedBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		ChannelEntities: map[params.Channel]map[string]*charm.URL{
			params.EdgeChannel: {
				storetesting.SearchSeries[0]: charm.MustParseURL("~who/django-0"),
				storetesting.SearchSeries[1]: charm.MustParseURL("~who/django-42"),
				"wily":                       charm.MustParseURL("~who/django-42"),
			},
		},
	},
}, {
	about:    "stable, multi series, publish edge",
	url:      MustParseResolvedURL("~who/django-47"),
	channels: []params.Channel{params.EdgeChannel},
	initialEntity: &mongodoc.Entity{
		URL:             charm.MustParseURL("~who/django-47"),
		SupportedSeries: []string{storetesting.SearchSeries[1], "wily", storetesting.SearchSeries[0]},
		Published: map[params.Channel]bool{
			params.StableChannel: true,
		},
	},
	initialBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		ChannelEntities: map[params.Channel]map[string]*charm.URL{
			params.StableChannel: {
				storetesting.SearchSeries[1]: charm.MustParseURL("~who/django-47"),
			},
		},
	},
	expectedEntity: &mongodoc.Entity{
		URL:             charm.MustParseURL("~who/django-47"),
		SupportedSeries: []string{storetesting.SearchSeries[1], "wily", storetesting.SearchSeries[0]},
		Published: map[params.Channel]bool{
			params.EdgeChannel:   true,
			params.StableChannel: true,
		},
	},
	expectedBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		ChannelEntities: map[params.Channel]map[string]*charm.URL{
			params.StableChannel: {
				storetesting.SearchSeries[1]: charm.MustParseURL("~who/django-47"),
			},
			params.EdgeChannel: {
				storetesting.SearchSeries[1]: charm.MustParseURL("~who/django-47"),
				"wily":                       charm.MustParseURL("~who/django-47"),
				storetesting.SearchSeries[0]: charm.MustParseURL("~who/django-47"),
			},
		},
	},
}, {
	about:    "unpublished, multi series, publish stable",
	url:      MustParseResolvedURL("~who/django-42"),
	channels: []params.Channel{params.StableChannel},
	initialEntity: &mongodoc.Entity{
		URL:             charm.MustParseURL("~who/django-42"),
		SupportedSeries: []string{storetesting.SearchSeries[1], "wily", storetesting.SearchSeries[0]},
	},
	initialBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
	},
	expectedEntity: &mongodoc.Entity{
		URL:             charm.MustParseURL("~who/django-42"),
		SupportedSeries: []string{storetesting.SearchSeries[1], "wily", storetesting.SearchSeries[0]},
		Published: map[params.Channel]bool{
			params.StableChannel: true,
		},
	},
	expectedBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		ChannelEntities: map[params.Channel]map[string]*charm.URL{
			params.StableChannel: {
				storetesting.SearchSeries[1]: charm.MustParseURL("~who/django-42"),
				"wily":                       charm.MustParseURL("~who/django-42"),
				storetesting.SearchSeries[0]: charm.MustParseURL("~who/django-42"),
			},
		},
	},
}, {
	about:    "edge, multi series, publish stable",
	url:      MustParseResolvedURL("~who/django-42"),
	channels: []params.Channel{params.StableChannel},
	initialEntity: &mongodoc.Entity{
		URL:             charm.MustParseURL("~who/django-42"),
		SupportedSeries: []string{"wily"},
		Published: map[params.Channel]bool{
			params.EdgeChannel: true,
		},
	},
	initialBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		ChannelEntities: map[params.Channel]map[string]*charm.URL{
			params.EdgeChannel: {
				storetesting.SearchSeries[1]: charm.MustParseURL("~who/django-0"),
			},
		},
	},
	expectedEntity: &mongodoc.Entity{
		URL:             charm.MustParseURL("~who/django-42"),
		SupportedSeries: []string{"wily"},
		Published: map[params.Channel]bool{
			params.EdgeChannel:   true,
			params.StableChannel: true,
		},
	},
	expectedBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		ChannelEntities: map[params.Channel]map[string]*charm.URL{
			params.StableChannel: {
				"wily": charm.MustParseURL("~who/django-42"),
			},
			params.EdgeChannel: {
				storetesting.SearchSeries[1]: charm.MustParseURL("~who/django-0"),
			},
		},
	},
}, {
	about:    "unpublished, multi series, publish beta",
	url:      MustParseResolvedURL("~who/django-42"),
	channels: []params.Channel{params.BetaChannel},
	initialEntity: &mongodoc.Entity{
		URL:             charm.MustParseURL("~who/django-42"),
		SupportedSeries: []string{storetesting.SearchSeries[1], "wily", storetesting.SearchSeries[0]},
	},
	initialBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
	},
	expectedEntity: &mongodoc.Entity{
		URL:             charm.MustParseURL("~who/django-42"),
		SupportedSeries: []string{storetesting.SearchSeries[1], "wily", storetesting.SearchSeries[0]},
		Published: map[params.Channel]bool{
			params.BetaChannel: true,
		},
	},
	expectedBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		ChannelEntities: map[params.Channel]map[string]*charm.URL{
			params.BetaChannel: {
				storetesting.SearchSeries[1]: charm.MustParseURL("~who/django-42"),
				"wily":                       charm.MustParseURL("~who/django-42"),
				storetesting.SearchSeries[0]: charm.MustParseURL("~who/django-42"),
			},
		},
	},
}, {
	about:    "beta, multi series, publish candidate",
	url:      MustParseResolvedURL("~who/django-42"),
	channels: []params.Channel{params.CandidateChannel},
	initialEntity: &mongodoc.Entity{
		URL:             charm.MustParseURL("~who/django-42"),
		SupportedSeries: []string{"wily"},
		Published: map[params.Channel]bool{
			params.BetaChannel: true,
		},
	},
	initialBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		ChannelEntities: map[params.Channel]map[string]*charm.URL{
			params.BetaChannel: {
				storetesting.SearchSeries[1]: charm.MustParseURL("~who/django-0"),
			},
		},
	},
	expectedEntity: &mongodoc.Entity{
		URL:             charm.MustParseURL("~who/django-42"),
		SupportedSeries: []string{"wily"},
		Published: map[params.Channel]bool{
			params.BetaChannel:      true,
			params.CandidateChannel: true,
		},
	},
	expectedBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		ChannelEntities: map[params.Channel]map[string]*charm.URL{
			params.CandidateChannel: {
				"wily": charm.MustParseURL("~who/django-42"),
			},
			params.BetaChannel: {
				storetesting.SearchSeries[1]: charm.MustParseURL("~who/django-0"),
			},
		},
	},
}, {
	about:    "stable, multi series, publish stable",
	url:      MustParseResolvedURL("~who/django-42"),
	channels: []params.Channel{params.StableChannel},
	initialEntity: &mongodoc.Entity{
		URL:             charm.MustParseURL("~who/django-42"),
		SupportedSeries: []string{storetesting.SearchSeries[1], "wily", storetesting.SearchSeries[0]},
		Published: map[params.Channel]bool{
			params.StableChannel: true,
		},
	},
	initialBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		ChannelEntities: map[params.Channel]map[string]*charm.URL{
			params.StableChannel: {
				storetesting.SearchSeries[0]: charm.MustParseURL("~who/django-1"),
				"quantal":                    charm.MustParseURL("~who/django-2"),
				"saucy":                      charm.MustParseURL("~who/django-3"),
				storetesting.SearchSeries[1]: charm.MustParseURL("~who/django-4"),
			},
		},
	},
	expectedEntity: &mongodoc.Entity{
		URL:             charm.MustParseURL("~who/django-42"),
		SupportedSeries: []string{storetesting.SearchSeries[1], "wily", storetesting.SearchSeries[0]},
		Published: map[params.Channel]bool{
			params.StableChannel: true,
		},
	},
	expectedBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		ChannelEntities: map[params.Channel]map[string]*charm.URL{
			params.StableChannel: {
				storetesting.SearchSeries[0]: charm.MustParseURL("~who/django-42"),
				"quantal":                    charm.MustParseURL("~who/django-2"),
				"saucy":                      charm.MustParseURL("~who/django-3"),
				storetesting.SearchSeries[1]: charm.MustParseURL("~who/django-42"),
				"wily":                       charm.MustParseURL("~who/django-42"),
			},
		},
	},
}, {
	about:    "bundle",
	url:      MustParseResolvedURL("~who/bundle/django-42"),
	channels: []params.Channel{params.StableChannel},
	initialEntity: &mongodoc.Entity{
		URL: charm.MustParseURL("~who/bundle/django-42"),
	},
	initialBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
	},
	expectedEntity: &mongodoc.Entity{
		URL: charm.MustParseURL("~who/bundle/django-42"),
		Published: map[params.Channel]bool{
			params.StableChannel: true,
		},
	},
	expectedBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		ChannelEntities: map[params.Channel]map[string]*charm.URL{
			params.StableChannel: {
				"bundle": charm.MustParseURL("~who/bundle/django-42"),
			},
		},
	},
}, {
	about: "unpublished, multi series, publish multiple channels",
	url:   MustParseResolvedURL("~who/django-42"),
	channels: []params.Channel{
		params.EdgeChannel,
		params.StableChannel,
		params.Channel("no-such"),
		params.UnpublishedChannel,
		params.CandidateChannel,
	},
	initialEntity: &mongodoc.Entity{
		URL:             charm.MustParseURL("~who/django-42"),
		SupportedSeries: []string{storetesting.SearchSeries[1], "wily"},
	},
	initialBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		ChannelEntities: map[params.Channel]map[string]*charm.URL{
			params.StableChannel: {
				"quantal":                    charm.MustParseURL("~who/django-1"),
				storetesting.SearchSeries[1]: charm.MustParseURL("~who/django-4"),
			},
			params.EdgeChannel: {
				"wily": charm.MustParseURL("~who/django-10"),
			},
		},
	},
	expectedEntity: &mongodoc.Entity{
		URL:             charm.MustParseURL("~who/django-42"),
		SupportedSeries: []string{storetesting.SearchSeries[1], "wily"},
		Published: map[params.Channel]bool{
			params.EdgeChannel:      true,
			params.CandidateChannel: true,
			params.StableChannel:    true,
		},
	},
	expectedBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		ChannelEntities: map[params.Channel]map[string]*charm.URL{
			params.EdgeChannel: {
				storetesting.SearchSeries[1]: charm.MustParseURL("~who/django-42"),
				"wily":                       charm.MustParseURL("~who/django-42"),
			},
			params.CandidateChannel: {
				storetesting.SearchSeries[1]: charm.MustParseURL("~who/django-42"),
				"wily":                       charm.MustParseURL("~who/django-42"),
			},
			params.StableChannel: {
				"quantal":                    charm.MustParseURL("~who/django-1"),
				storetesting.SearchSeries[1]: charm.MustParseURL("~who/django-42"),
				"wily":                       charm.MustParseURL("~who/django-42"),
			},
		},
	},
}, {
	about:    "not found",
	url:      MustParseResolvedURL("~who/" + storetesting.SearchSeries[1] + "/no-such-42"),
	channels: []params.Channel{params.EdgeChannel},
	initialEntity: &mongodoc.Entity{
		URL: charm.MustParseURL("~who/" + storetesting.SearchSeries[1] + "/django-42"),
	},
	initialBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
	},
	expectedErr: `entity not found`,
}, {
	about:    "no valid channels provided",
	url:      MustParseResolvedURL("~who/" + storetesting.SearchSeries[1] + "/django-42"),
	channels: []params.Channel{params.Channel("not-valid")},
	initialEntity: &mongodoc.Entity{
		URL: charm.MustParseURL("~who/" + storetesting.SearchSeries[1] + "/django-42"),
	},
	initialBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
	},
	expectedErr: `cannot update "cs:~who/` + storetesting.SearchSeries[1] + `/django-42": no valid channels provided`,
}, {
	about:    "unpublished channel provided",
	url:      MustParseResolvedURL("~who/" + storetesting.SearchSeries[1] + "/django-42"),
	channels: []params.Channel{params.UnpublishedChannel},
	initialEntity: &mongodoc.Entity{
		URL: charm.MustParseURL("~who/" + storetesting.SearchSeries[1] + "/django-42"),
	},
	initialBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
	},
	expectedErr: `cannot update "cs:~who/` + storetesting.SearchSeries[1] + `/django-42": no valid channels provided`,
}}

func (s *StoreSuite) TestPublish(c *gc.C) {
	store := s.newStore(c, true)
	defer store.Close()

	for i, test := range publishTests {
		c.Logf("test %d: %s", i, test.about)

		// Remove existing entities and base entities.
		_, err := store.DB.Entities().RemoveAll(nil)
		c.Assert(err, gc.Equals, nil)
		_, err = store.DB.BaseEntities().RemoveAll(nil)
		c.Assert(err, gc.Equals, nil)
		// Insert the existing entity.
		err = store.DB.Entities().Insert(denormalizedEntity(test.initialEntity))
		c.Assert(err, gc.Equals, nil)
		// Insert the existing base entity.
		err = store.DB.BaseEntities().Insert(test.initialBaseEntity)
		c.Assert(err, gc.Equals, nil)

		// Publish the entity.
		err = store.Publish(test.url, nil, test.channels...)
		if test.expectedErr != "" {
			c.Assert(err, gc.ErrorMatches, test.expectedErr)
			continue
		}
		c.Assert(err, gc.Equals, nil)
		entity, err := store.FindEntity(test.url, nil)
		c.Assert(err, gc.Equals, nil)
		c.Assert(entity, jc.DeepEquals, denormalizedEntity(test.expectedEntity))
		baseEntity, err := store.FindBaseEntity(&test.url.URL, nil)
		c.Assert(err, gc.Equals, nil)
		c.Assert(storetesting.NormalizeBaseEntity(baseEntity), jc.DeepEquals, storetesting.NormalizeBaseEntity(test.expectedBaseEntity))
	}
}

func (s *StoreSuite) TestPublishWithFailedESInsert(c *gc.C) {
	// Make an elastic search with a non-existent address,
	// so that will try to add the charm there, but fail.
	esdb := &elasticsearch.Database{
		Addr: "0.1.2.3:0123",
	}

	store := s.newStore(c, false)
	defer store.Close()
	store.ES = &SearchIndex{esdb, "no-index"}

	url := router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[0]+"/wordpress-12", -1)
	err := store.AddCharmWithArchive(url, storetesting.Charms.CharmDir("wordpress"))
	c.Assert(err, gc.Equals, nil)
	err = store.Publish(url, nil, params.StableChannel)
	c.Assert(err, gc.ErrorMatches, "cannot index cs:~charmers/"+storetesting.SearchSeries[0]+"/wordpress-12 to ElasticSearch: .*")
}

func (s *StoreSuite) TestDeleteEntity(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()
	url := router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[0]+"/wordpress-12", -1)
	err := store.AddCharmWithArchive(url, storetesting.NewCharm(&charm.Meta{
		Series: []string{storetesting.SearchSeries[0]},
	}))
	c.Assert(err, gc.Equals, nil)
	url1 := *url
	url1.URL.Revision = 13
	err = store.AddCharmWithArchive(&url1, storetesting.NewCharm(&charm.Meta{
		Summary: "another piece of content",
		Series:  []string{storetesting.SearchSeries[0]},
	}))
	c.Assert(err, gc.Equals, nil)

	entity, err := store.FindEntity(url, nil)
	c.Assert(err, gc.Equals, nil)

	err = store.DeleteEntity(url)
	c.Assert(err, gc.Equals, nil)

	_, err = store.FindEntity(url, nil)
	c.Assert(errgo.Cause(err), gc.Equals, params.ErrNotFound)

	// Run blobstore garbage collection and check that
	// the blob and the pre-v5 compatibility blob have been
	// removed.
	err = store.BlobStoreGC(time.Now())
	c.Assert(err, gc.Equals, nil)

	_, _, err = store.BlobStore.Open(entity.BlobHash, nil)
	c.Assert(errgo.Cause(err), gc.Equals, blobstore.ErrNotFound)

	_, _, err = store.BlobStore.Open(entity.PreV5BlobExtraHash, nil)
	c.Assert(errgo.Cause(err), gc.Equals, blobstore.ErrNotFound)
}

func (s *StoreSuite) TestDeleteEntityWithOnlyOneRevision(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()
	url := router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[0]+"/wordpress-12", -1)
	err := store.AddCharmWithArchive(url, storetesting.NewCharm(&charm.Meta{
		Series: []string{storetesting.SearchSeries[0]},
	}))
	c.Assert(err, gc.Equals, nil)

	err = store.DeleteEntity(url)
	c.Assert(err, gc.ErrorMatches, `cannot delete last revision of charm or bundle`)
}

func (s *StoreSuite) TestDeleteEntityWithPublishedRevision(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()
	url := router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[0]+"/wordpress-12", -1)
	err := store.AddCharmWithArchive(url, storetesting.NewCharm(&charm.Meta{
		Series: []string{storetesting.SearchSeries[0]},
	}))
	c.Assert(err, gc.Equals, nil)
	err = store.Publish(url, nil, params.EdgeChannel, params.BetaChannel)
	c.Assert(err, gc.Equals, nil)
	url1 := *url
	url1.URL.Revision = 13
	err = store.AddCharmWithArchive(&url1, storetesting.NewCharm(&charm.Meta{
		Series: []string{storetesting.SearchSeries[0]},
	}))
	c.Assert(err, gc.Equals, nil)

	err = store.DeleteEntity(url)
	c.Assert(err, gc.ErrorMatches, `cannot delete "cs:~charmers/`+storetesting.SearchSeries[0]+`/wordpress-12" because it is the current revision in channels \[beta edge\]`)

	// Check that it really hasn't been deleted.
	_, err = store.FindEntity(url, nil)
	c.Assert(err, gc.Equals, nil)
}

func (s *StoreSuite) TestGC(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()

	// Add some charms and resources.

	id1 := router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[0]+"/wordpress-1", -1)
	err := store.AddCharmWithArchive(id1, storetesting.NewCharm(&charm.Meta{
		Summary: "charm that will be deleted",
		Series:  []string{storetesting.SearchSeries[0], storetesting.SearchSeries[1]},
	}))
	c.Assert(err, gc.Equals, nil)

	id2 := router.MustNewResolvedURL("~charmers/"+storetesting.SearchSeries[0]+"/wordpress-2", -1)
	err = store.AddCharmWithArchive(id2, storetesting.NewCharm(&charm.Meta{
		Summary: "charm that will not be deleted",
		Series:  []string{storetesting.SearchSeries[0], storetesting.SearchSeries[1]},
	}))
	c.Assert(err, gc.Equals, nil)

	id3 := MustParseResolvedURL("cs:~charmers/" + storetesting.SearchSeries[0] + "/withresource-1")
	err = store.AddCharmWithArchive(id3, storetesting.NewCharm(storetesting.MetaWithResources(nil, "someResource")))
	c.Assert(err, gc.Equals, nil)
	contents := []string{
		"123456789 123456789 ",
		"abcdefghijklmnopqrstuvxwyz",
	}
	uid := putMultipart(c, store.BlobStore, time.Time{}, contents...)
	_, err = store.AddResourceWithUploadId(id3, "someResource", -1, uid)
	c.Assert(err, gc.Equals, nil)

	contents = []string{
		"123456789 123456789 ",
		"ABCDEFGHIJKLMNOPQURSTUVWXYZ",
	}
	uid = putMultipart(c, store.BlobStore, time.Time{}, contents...)
	resource2, err := store.AddResourceWithUploadId(id3, "someResource", -1, uid)
	c.Assert(err, gc.Equals, nil)

	type blobInfo struct {
		hash  string
		about string
		keep  bool
	}
	var blobs []blobInfo

	for _, id := range []*router.ResolvedURL{
		id1,
		id2,
		id3,
	} {
		{
			entity, err := store.FindEntity(id, nil)
			c.Assert(err, gc.Equals, nil)
			willKeep := id != id1
			blobs = append(blobs, blobInfo{
				hash:  entity.BlobHash,
				about: fmt.Sprintf("%s main blob", id),
				keep:  willKeep,
			})
			if entity.PreV5BlobExtraHash != "" {
				blobs = append(blobs, blobInfo{
					hash:  entity.PreV5BlobExtraHash,
					about: fmt.Sprintf("%s extra blob", id),
					keep:  willKeep,
				})
			}
		}
	}
	blobs = append(blobs, blobInfo{
		hash:  hashOfString("123456789 123456789 "),
		about: fmt.Sprintf("content 123456789 123456789 "),
		keep:  true,
	})
	blobs = append(blobs, blobInfo{
		hash:  hashOfString("abcdefghijklmnopqrstuvxwyz"),
		about: fmt.Sprintf("content abcdefghijklmnopqrstuvxwyz"),
		keep:  true,
	})
	blobs = append(blobs, blobInfo{
		hash:  hashOfString("ABCDEFGHIJKLMNOPQURSTUVWXYZ"),
		about: fmt.Sprintf("content ABCDEFGHIJKLMNOPQURSTUVWXYZ"),
		keep:  false,
	})

	// First check we can get all the blobs.
	for _, info := range blobs {
		r, _, err := store.BlobStore.Open(info.hash, nil)
		c.Assert(err, gc.Equals, nil, gc.Commentf("%s", info.about))
		r.Close()
	}

	// Then remove an entity and a resource.
	err = store.DeleteEntity(id1)
	c.Assert(err, gc.Equals, nil)
	err = store.DB.Resources().Remove(bson.D{{
		"baseurl", resource2.BaseURL,
	}, {
		"name", resource2.Name,
	}, {
		"revision", resource2.Revision,
	}})
	c.Assert(err, gc.Equals, nil)

	// Run the garbage collector and check that
	// the correct blobs have been removed.

	err = store.BlobStoreGC(time.Now())
	c.Assert(err, gc.Equals, nil)

	for _, info := range blobs {
		r, _, err := store.BlobStore.Open(info.hash, nil)
		if info.keep {
			c.Assert(err, gc.Equals, nil, gc.Commentf("%s", info.about))
			r.Close()
		} else {
			c.Assert(errgo.Cause(err), gc.Equals, blobstore.ErrNotFound, gc.Commentf("%s", info.about))
		}
	}
}

func urlStrings(urls []*charm.URL) []string {
	urlStrs := make([]string, len(urls))
	for i, url := range urls {
		urlStrs[i] = url.String()
	}
	return urlStrs
}

// MustParseResolvedURL parses a resolved URL in string form, with
// the optional promulgated revision preceding the entity URL
// separated by a space.
func MustParseResolvedURL(urlStr string) *router.ResolvedURL {
	s := strings.Fields(urlStr)
	promRev := -1
	switch len(s) {
	default:
		panic(fmt.Errorf("invalid resolved URL string %q", urlStr))
	case 2:
		var err error
		promRev, err = strconv.Atoi(s[0])
		if err != nil || promRev < 0 {
			panic(fmt.Errorf("invalid resolved URL string %q", urlStr))
		}
	case 1:
	}
	url := charm.MustParseURL(s[len(s)-1])
	if url.User == "" {
		panic("resolved URL with no user")
	}
	if url.Revision == -1 {
		panic("resolved URL with no revision")
	}
	return &router.ResolvedURL{
		URL:                 *url,
		PromulgatedRevision: promRev,
	}
}

func MustParseResolvedURLs(urlStrs []string) []*router.ResolvedURL {
	urls := make([]*router.ResolvedURL, len(urlStrs))
	for i, u := range urlStrs {
		urls[i] = MustParseResolvedURL(u)
	}
	return urls
}

func hashOfReader(r io.Reader) string {
	hash := sha512.New384()
	_, err := io.Copy(hash, r)
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf("%x", hash.Sum(nil))
}

func hashOfString(s string) string {
	return hashOfReader(strings.NewReader(s))
}

func getSizeAndHashes(c interface{}) (int64, string, string) {
	var r io.ReadWriter
	var err error
	switch c := c.(type) {
	case ArchiverTo:
		r = new(bytes.Buffer)
		err = c.ArchiveTo(r)
	case *charm.BundleArchive:
		r, err = os.Open(c.Path)
	case *charm.CharmArchive:
		r, err = os.Open(c.Path)
	default:
		panic(fmt.Sprintf("unable to get size and hash for type %T", c))
	}
	if err != nil {
		panic(err)
	}
	hash := blobstore.NewHash()
	hash256 := sha256.New()
	size, err := io.Copy(io.MultiWriter(hash, hash256), r)
	if err != nil {
		panic(err)
	}
	return size, fmt.Sprintf("%x", hash.Sum(nil)), fmt.Sprintf("%x", hash256.Sum(nil))
}

// testingBundle implements charm.Bundle, allowing tests
// to create a bundle with custom data.
type testingBundle struct {
	data *charm.BundleData
}

func (b *testingBundle) Data() *charm.BundleData {
	return b.data
}

func (b *testingBundle) ReadMe() string {
	// For the purposes of this implementation, the charm readme is not
	// relevant.
	return ""
}

const fakeContent = "fake content"

// Define fake blob attributes to be used in tests.
var fakeBlobSize, fakeBlobHash = func() (int64, string) {
	b := []byte(fakeContent)
	h := blobstore.NewHash()
	h.Write(b)
	return int64(len(b)), fmt.Sprintf("%x", h.Sum(nil))
}()

func entity(url, purl string, supportedSeries ...string) *mongodoc.Entity {
	id := charm.MustParseURL(url)
	var pid *charm.URL
	if purl != "" {
		pid = charm.MustParseURL(purl)
	}
	e := &mongodoc.Entity{
		URL:             id,
		PromulgatedURL:  pid,
		SupportedSeries: supportedSeries,
	}
	denormalizeEntity(e)
	return e
}

func baseEntity(url string, promulgated bool) *mongodoc.BaseEntity {
	id := charm.MustParseURL(url)
	return &mongodoc.BaseEntity{
		URL:             id,
		Name:            id.Name,
		User:            id.User,
		Promulgated:     mongodoc.IntBool(promulgated),
		ChannelEntities: make(map[params.Channel]map[string]*charm.URL),
	}
}

// denormalizedEntity is a convenience function that returns
// a copy of e with its denormalized fields filled out.
func denormalizedEntity(e *mongodoc.Entity) *mongodoc.Entity {
	e1 := *e
	denormalizeEntity(&e1)
	return &e1
}
