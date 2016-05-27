// Copyright 2014 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package charmstore // import "gopkg.in/juju/charmstore.v5-unstable/internal/charmstore"

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

	jc "github.com/juju/testing/checkers"
	gc "gopkg.in/check.v1"
	"gopkg.in/errgo.v1"
	"gopkg.in/juju/charm.v6-unstable"
	"gopkg.in/juju/charmrepo.v2-unstable/csclient/params"
	"gopkg.in/mgo.v2/bson"
	"gopkg.in/natefinch/lumberjack.v2"

	"gopkg.in/juju/charmstore.v5-unstable/audit"
	"gopkg.in/juju/charmstore.v5-unstable/elasticsearch"
	"gopkg.in/juju/charmstore.v5-unstable/internal/blobstore"
	"gopkg.in/juju/charmstore.v5-unstable/internal/mongodoc"
	"gopkg.in/juju/charmstore.v5-unstable/internal/router"
	"gopkg.in/juju/charmstore.v5-unstable/internal/storetesting"
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
	inStore: []string{"23 cs:~charmers/precise/wordpress-23"},
	expand:  "wordpress",
	expect:  []string{"23 cs:~charmers/precise/wordpress-23"},
}, {
	inStore: []string{"23 cs:~charmers/precise/wordpress-23", "24 cs:~charmers/precise/wordpress-24", "25 cs:~charmers/precise/wordpress-25"},
	expand:  "wordpress",
	expect:  []string{"23 cs:~charmers/precise/wordpress-23", "24 cs:~charmers/precise/wordpress-24", "25 cs:~charmers/precise/wordpress-25"},
}, {
	inStore: []string{"23 cs:~charmers/precise/wordpress-23", "24 cs:~charmers/precise/wordpress-24", "25 cs:~charmers/precise/wordpress-25"},
	expand:  "~charmers/precise/wordpress-24",
	expect:  []string{"24 cs:~charmers/precise/wordpress-24"},
}, {
	inStore: []string{"23 cs:~charmers/precise/wordpress-23", "24 cs:~charmers/precise/wordpress-24", "25 cs:~charmers/precise/wordpress-25"},
	expand:  "~charmers/precise/wordpress-25",
	expect:  []string{"25 cs:~charmers/precise/wordpress-25"},
}, {
	inStore: []string{"23 cs:~charmers/precise/wordpress-23", "24 cs:~charmers/trusty/wordpress-24", "25 cs:~charmers/precise/wordpress-25"},
	expand:  "precise/wordpress",
	expect:  []string{"23 cs:~charmers/precise/wordpress-23", "25 cs:~charmers/precise/wordpress-25"},
}, {
	inStore: []string{"23 cs:~charmers/precise/wordpress-23", "24 cs:~charmers/trusty/wordpress-24", "434 cs:~charmers/foo/varnish-434"},
	expand:  "wordpress",
	expect:  []string{"23 cs:~charmers/precise/wordpress-23", "24 cs:~charmers/trusty/wordpress-24"},
}, {
	inStore: []string{"23 cs:~charmers/precise/wordpress-23", "23 cs:~charmers/trusty/wordpress-23", "24 cs:~charmers/trusty/wordpress-24"},
	expand:  "wordpress-23",
	expect:  []string{},
}, {
	inStore: []string{"cs:~user/precise/wordpress-23", "cs:~user/trusty/wordpress-23"},
	expand:  "~user/precise/wordpress",
	expect:  []string{"cs:~user/precise/wordpress-23"},
}, {
	inStore: []string{"cs:~user/precise/wordpress-23", "cs:~user/trusty/wordpress-23"},
	expand:  "~user/wordpress",
	expect:  []string{"cs:~user/precise/wordpress-23", "cs:~user/trusty/wordpress-23"},
}, {
	inStore: []string{"23 cs:~charmers/precise/wordpress-23", "24 cs:~charmers/trusty/wordpress-24", "434 cs:~charmers/foo/varnish-434"},
	expand:  "precise/wordpress-23",
	expect:  []string{"23 cs:~charmers/precise/wordpress-23"},
}, {
	inStore: []string{"23 cs:~charmers/precise/wordpress-23", "24 cs:~charmers/trusty/wordpress-24", "434 cs:~charmers/foo/varnish-434"},
	expand:  "arble",
	expect:  []string{},
}, {
	inStore: []string{"23 cs:~charmers/multi-series-23", "24 cs:~charmers/multi-series-24"},
	expand:  "multi-series",
	expect:  []string{"23 cs:~charmers/multi-series-23", "24 cs:~charmers/multi-series-24"},
}, {
	inStore: []string{"23 cs:~charmers/multi-series-23", "24 cs:~charmers/multi-series-24"},
	expand:  "trusty/multi-series",
	expect:  []string{"23 cs:~charmers/multi-series-23", "24 cs:~charmers/multi-series-24"},
}, {
	inStore: []string{"23 cs:~charmers/multi-series-23", "24 cs:~charmers/multi-series-24"},
	expand:  "multi-series-24",
	expect:  []string{"24 cs:~charmers/multi-series-24"},
}, {
	inStore: []string{"23 cs:~charmers/multi-series-23", "24 cs:~charmers/multi-series-24"},
	expand:  "trusty/multi-series-24",
	expect:  []string{"24 cs:~charmers/multi-series-24"},
}, {
	inStore: []string{"1 cs:~charmers/multi-series-23", "2 cs:~charmers/multi-series-24"},
	expand:  "trusty/multi-series-1",
	expect:  []string{"1 cs:~charmers/multi-series-23"},
}, {
	inStore: []string{"1 cs:~charmers/multi-series-23", "2 cs:~charmers/multi-series-24"},
	expand:  "multi-series-23",
	expect:  []string{},
}, {
	inStore: []string{"1 cs:~charmers/multi-series-23", "2 cs:~charmers/multi-series-24"},
	expand:  "cs:~charmers/utopic/multi-series-23",
	expect:  []string{"1 cs:~charmers/multi-series-23"},
}, {
	inStore: []string{},
	expand:  "precise/wordpress-23",
	expect:  []string{},
}}

func (s *StoreSuite) testURLFinding(c *gc.C, check func(store *Store, expand *charm.URL, expect []*router.ResolvedURL)) {
	charms := make(map[string]*charm.CharmDir)
	store := s.newStore(c, false)
	defer store.Close()
	for i, test := range urlFindingTests {
		c.Logf("test %d: %q from %q", i, test.expand, test.inStore)
		_, err := store.DB.Entities().RemoveAll(nil)
		c.Assert(err, gc.IsNil)
		urls := MustParseResolvedURLs(test.inStore)
		for _, url := range urls {
			name := url.URL.Name
			if charms[name] == nil {
				charms[name] = storetesting.Charms.CharmDir(name)
			}
			err := store.AddCharmWithArchive(url, charms[name])
			c.Assert(err, gc.IsNil)
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
	c.Assert(err, gc.IsNil)
	defer p.Close()

	// Instances within the limit can be acquired
	// instantly without error.
	store, err := p.RequestStore()
	c.Assert(err, gc.IsNil)
	store.Close()

	// Check that when we get another instance,
	// we reuse the original.
	store1, err := p.RequestStore()
	c.Assert(err, gc.IsNil)
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
	c.Assert(err, gc.IsNil)
	defer p.Close()
	store, err := p.RequestStore()
	c.Assert(err, gc.IsNil)

	// Start a goroutine that will close the Store after a short period.
	go func() {
		time.Sleep(time.Millisecond)
		store.Close()
	}()
	store1, err := p.RequestStore()
	c.Assert(err, gc.IsNil)
	c.Assert(store1, gc.Equals, store)
	store1.Close()
}

func (s *StoreSuite) TestRequestStoreLimitCanBeExceeded(c *gc.C) {
	config := ServerParams{
		HTTPRequestWaitDuration: 5 * time.Second,
		MaxMgoSessions:          1,
	}
	p, err := NewPool(s.Session.DB("juju_test"), nil, nil, config)
	c.Assert(err, gc.IsNil)
	defer p.Close()
	store, err := p.RequestStore()
	c.Assert(err, gc.IsNil)
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
	c.Assert(err, gc.IsNil)
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
	c.Assert(err, gc.IsNil)
	defer p.Close()

	// Acquire an instance.
	store, err := p.RequestStore()
	c.Assert(err, gc.IsNil)
	defer store.Close()

	// Acquire another instance, exceeding the limit,
	// and put it back.
	store1 := p.Store()
	c.Assert(err, gc.IsNil)
	store1.Close()

	// We should still be unable to acquire another
	// store for a request because we're still
	// at the request limit.
	_, err = p.RequestStore()
	c.Assert(errgo.Cause(err), gc.Equals, ErrTooManySessions)
}

func (s *StoreSuite) TestPoolDoubleClose(c *gc.C) {
	p, err := NewPool(s.Session.DB("juju_test"), nil, nil, ServerParams{})
	c.Assert(err, gc.IsNil)
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
		c.Assert(err, gc.IsNil)
		if expand.User == "" {
			sort.Sort(entitiesByPromulgatedURL(gotEntities))
		} else {
			sort.Sort(entitiesByURL(gotEntities))
		}
		c.Assert(gotEntities, gc.HasLen, len(expect))
		for i, url := range expect {
			c.Assert(gotEntities[i], jc.DeepEquals, &mongodoc.Entity{
				URL:            &url.URL,
				PromulgatedURL: url.PromulgatedURL(),
			}, gc.Commentf("index %d", i))
		}

		// check FindEntities works when retrieving all fields.
		gotEntities, err = store.FindEntities(expand, nil)
		c.Assert(err, gc.IsNil)
		if expand.User == "" {
			sort.Sort(entitiesByPromulgatedURL(gotEntities))
		} else {
			sort.Sort(entitiesByURL(gotEntities))
		}
		c.Assert(gotEntities, gc.HasLen, len(expect))
		for i, url := range expect {
			var entity mongodoc.Entity
			err := store.DB.Entities().FindId(&url.URL).One(&entity)
			c.Assert(err, gc.IsNil)
			c.Assert(gotEntities[i], jc.DeepEquals, &entity)
		}
	})
}

func (s *StoreSuite) TestFindEntity(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()

	rurl := MustParseResolvedURL("cs:~charmers/precise/wordpress-5")
	err := store.AddCharmWithArchive(rurl, storetesting.Charms.CharmDir("wordpress"))
	c.Assert(err, gc.IsNil)

	entity0, err := store.FindEntity(rurl, nil)
	c.Assert(err, gc.IsNil)
	c.Assert(entity0, gc.NotNil)
	c.Assert(entity0.Size, gc.Not(gc.Equals), 0)

	// Check that the field selector works.
	entity2, err := store.FindEntity(rurl, FieldSelector("blobhash"))
	c.Assert(err, gc.IsNil)
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
			params.DevelopmentChannel: {
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
	stored: []string{"42 cs:~charmers/utopic/mysql-42", "~who/precise/mysql-47"},
	url:    "~who/precise/mysql-0",
	fields: []string{"user"},
	expect: &mongodoc.BaseEntity{
		URL:  charm.MustParseURL("~who/mysql"),
		User: "who",
	},
}, {
	about:  "entity found, partial url, only the ACLs",
	stored: []string{"42 cs:~charmers/utopic/mysql-42", "~who/trusty/mysql-47"},
	url:    "~who/mysql-42",
	fields: []string{"channelacls"},
	expect: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/mysql"),
		ChannelACLs: map[params.Channel]mongodoc.ACL{
			params.UnpublishedChannel: {
				Read:  []string{"who"},
				Write: []string{"who"},
			},
			params.DevelopmentChannel: {
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
	stored: []string{"42 cs:~charmers/utopic/mysql-42", "~who/trusty/mysql-47"},
	url:    "rails",
}, {
	about:  "entity not found, user",
	stored: []string{"42 cs:~charmers/utopic/mysql-42", "~who/trusty/mysql-47"},
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
			c.Assert(err, gc.IsNil)
		}

		// Find the entity.
		id := charm.MustParseURL(test.url)
		baseEntity, err := store.FindBaseEntity(id, FieldSelector(test.fields...))
		if test.expect == nil {
			// We don't expect the entity to be found.
			c.Assert(errgo.Cause(err), gc.Equals, params.ErrNotFound)
			c.Assert(baseEntity, gc.IsNil)
		} else {
			c.Assert(err, gc.IsNil)
			c.Assert(storetesting.NormalizeBaseEntity(baseEntity), jc.DeepEquals, storetesting.NormalizeBaseEntity(test.expect))
		}

		// Remove all the entities from the store.
		_, err = store.DB.Entities().RemoveAll(nil)
		c.Assert(err, gc.IsNil)
		_, err = store.DB.BaseEntities().RemoveAll(nil)
		c.Assert(err, gc.IsNil)
	}
}

func (s *StoreSuite) TestAddCharmsWithTheSameBaseEntity(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()

	// Add a charm to the database.
	ch := storetesting.Charms.CharmDir("wordpress")
	url := router.MustNewResolvedURL("~charmers/trusty/wordpress-12", 12)
	err := store.AddCharmWithArchive(url, ch)
	c.Assert(err, gc.IsNil)

	// Add a second charm to the database, sharing the same base URL.
	err = store.AddCharmWithArchive(router.MustNewResolvedURL("~charmers/utopic/wordpress-13", -1), ch)
	c.Assert(err, gc.IsNil)

	// Ensure a single base entity has been created.
	num, err := store.DB.BaseEntities().Count()
	c.Assert(err, gc.IsNil)
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
				Charm:    "cs:trusty/mysql-0",
				NumUnits: 0,
			},
		},
	},
}, {
	about: "a single unit",
	data: &charm.BundleData{
		Applications: map[string]*charm.ApplicationSpec{
			"wordpress": {
				Charm:    "cs:trusty/wordpress-42",
				NumUnits: 1,
			},
			"mysql": {
				Charm:    "cs:trusty/mysql-47",
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
		c.Assert(err, gc.IsNil)

		// Retrieve the bundle from the database.
		var doc mongodoc.Entity
		err = entities.FindId(&url.URL).One(&doc)
		c.Assert(err, gc.IsNil)

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
				Charm:    "cs:trusty/wordpress-0",
				NumUnits: 0,
			},
		},
	},
}, {
	about: "a single machine (no placement)",
	data: &charm.BundleData{
		Applications: map[string]*charm.ApplicationSpec{
			"mysql": {
				Charm:    "cs:trusty/mysql-42",
				NumUnits: 1,
			},
			"wordpress": {
				Charm:    "cs:trusty/wordpress-47",
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
				Charm:    "cs:trusty/mysql-42",
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
				Charm:    "cs:trusty/mysql-42",
				NumUnits: 1,
				To:       []string{"1"},
			},
			"wordpress": {
				Charm:    "cs:trusty/wordpress-47",
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
				Charm:    "cs:trusty/mysql-42",
				NumUnits: 1,
			},
			"wordpress": {
				Charm:    "cs:trusty/wordpress-47",
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
				Charm:    "cs:trusty/mysql-42",
				NumUnits: 1,
				To:       []string{"1"},
			},
			"wordpress": {
				Charm:    "cs:trusty/wordpress-47",
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
				Charm:    "cs:trusty/mysql-42",
				NumUnits: 1,
				To:       []string{"1"},
			},
			"wordpress": {
				Charm:    "cs:trusty/wordpress-47",
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
				Charm:    "cs:trusty/mysql-42",
				NumUnits: 2,
			},
			"wordpress": {
				Charm:    "cs:trusty/wordpress-47",
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
				Charm:    "cs:trusty/mysql-42",
				NumUnits: 2,
				To:       []string{"1", "2"},
			},
			"wordpress": {
				Charm:    "cs:trusty/wordpress-47",
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
				Charm:    "cs:trusty/mysql-42",
				NumUnits: 1,
				To:       []string{"1"},
			},
			"wordpress": {
				Charm:    "cs:trusty/wordpress-47",
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
				Charm:    "cs:trusty/mysql-42",
				NumUnits: 1,
				To:       []string{"1"},
			},
			"wordpress": {
				Charm:    "cs:trusty/wordpress-47",
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
				Charm:    "cs:trusty/mysql-42",
				NumUnits: 3,
			},
			"wordpress": {
				Charm:    "cs:trusty/wordpress-47",
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
				Charm:    "cs:trusty/wordpress-47",
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
		err := test.data.Verify(nil, nil)
		c.Assert(err, gc.IsNil)
		// Add the bundle used for this test.
		b := storetesting.NewBundle(test.data)
		s.addRequiredCharms(c, b)
		err = store.AddBundleWithArchive(url, b)
		c.Assert(err, gc.IsNil)

		// Retrieve the bundle from the database.
		var doc mongodoc.Entity
		err = entities.FindId(&url.URL).One(&doc)
		c.Assert(err, gc.IsNil)

		c.Assert(*doc.BundleMachineCount, gc.Equals, test.expectMachines)
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
		URL:                 *url.WithChannel(""),
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

func (s *StoreSuite) TestOpenBlob(c *gc.C) {
	charmArchive := storetesting.Charms.CharmArchive(c.MkDir(), "wordpress")
	store := s.newStore(c, false)
	defer store.Close()
	url := router.MustNewResolvedURL("cs:~charmers/precise/wordpress-23", 23)
	err := store.AddCharmWithArchive(url, charmArchive)
	c.Assert(err, gc.IsNil)

	f, err := os.Open(charmArchive.Path)
	c.Assert(err, gc.IsNil)
	defer f.Close()
	expectHash := hashOfReader(f)

	blob, err := store.OpenBlob(url)
	c.Assert(err, gc.IsNil)
	defer blob.Close()

	c.Assert(hashOfReader(blob), gc.Equals, expectHash)
	c.Assert(blob.Hash, gc.Equals, expectHash)

	info, err := f.Stat()
	c.Assert(err, gc.IsNil)
	c.Assert(blob.Size, gc.Equals, info.Size())
}

func (s *StoreSuite) TestOpenBlobPreV5(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()
	ch := storetesting.NewCharm(storetesting.MetaWithSupportedSeries(nil, "trusty", "precise"))

	url := router.MustNewResolvedURL("cs:~charmers/multi-series-23", 23)
	err := store.AddCharmWithArchive(url, ch)
	c.Assert(err, gc.IsNil)

	blob, err := store.OpenBlobPreV5(url)
	c.Assert(err, gc.IsNil)
	defer blob.Close()

	data, err := ioutil.ReadAll(blob)
	c.Assert(err, gc.IsNil)
	preV5Ch, err := charm.ReadCharmArchiveBytes(data)
	c.Assert(err, gc.IsNil)

	// Check that the hashes and sizes are consistent with the data
	// we've read.
	c.Assert(blob.Hash, gc.Equals, fmt.Sprintf("%x", sha512.Sum384(data)))
	c.Assert(blob.Size, gc.Equals, int64(len(data)))

	entity, err := store.FindEntity(url, nil)
	c.Assert(err, gc.IsNil)

	c.Assert(entity.PreV5BlobHash, gc.Equals, blob.Hash)
	c.Assert(entity.PreV5BlobHash256, gc.Equals, fmt.Sprintf("%x", sha256.Sum256(data)))
	c.Assert(entity.PreV5BlobSize, gc.Equals, blob.Size)

	c.Assert(preV5Ch.Meta().Series, gc.HasLen, 0)

	// Sanity check that the series really are in the post-v5 blob.
	blob, err = store.OpenBlob(url)
	c.Assert(err, gc.IsNil)
	defer blob.Close()

	data, err = ioutil.ReadAll(blob)
	c.Assert(err, gc.IsNil)

	postV5Ch, err := charm.ReadCharmArchiveBytes(data)
	c.Assert(err, gc.IsNil)

	c.Assert(postV5Ch.Meta().Series, jc.DeepEquals, []string{"trusty", "precise"})
}

func (s *StoreSuite) TestOpenBlobPreV5WithMultiSeriesCharmInSingleSeriesId(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()
	ch := storetesting.NewCharm(storetesting.MetaWithSupportedSeries(nil, "trusty", "precise"))

	url := router.MustNewResolvedURL("cs:~charmers/precise/multi-series-23", 23)
	err := store.AddCharmWithArchive(url, ch)
	c.Assert(err, gc.IsNil)

	blob, err := store.OpenBlobPreV5(url)
	c.Assert(err, gc.IsNil)
	defer blob.Close()

	data, err := ioutil.ReadAll(blob)
	c.Assert(err, gc.IsNil)
	preV5Ch, err := charm.ReadCharmArchiveBytes(data)
	c.Assert(err, gc.IsNil)

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
	c.Assert(err, gc.IsNil)
	err = store.AddLog(&errorData, mongodoc.ErrorLevel, mongodoc.IngestionType, urls)
	c.Assert(err, gc.IsNil)
	afterAdding := time.Now().Add(time.Second)

	// Retrieve the logs from the store.
	var docs []mongodoc.Log
	err = store.DB.Logs().Find(nil).Sort("_id").All(&docs)
	c.Assert(err, gc.IsNil)
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
		charm.MustParseURL("trusty/mysql-42"),
		charm.MustParseURL("~who/utopic/wordpress"),
	})
	c.Assert(err, gc.IsNil)

	// Retrieve the log from the store.
	var doc mongodoc.Log
	err = store.DB.Logs().Find(nil).One(&doc)
	c.Assert(err, gc.IsNil)

	// The log includes the base URLs.
	c.Assert(doc.URLs, jc.DeepEquals, []*charm.URL{
		charm.MustParseURL("trusty/mysql-42"),
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
		charm.MustParseURL("trusty/mysql-42"),
		charm.MustParseURL("mysql"),
		charm.MustParseURL("trusty/mysql-42"),
		charm.MustParseURL("mysql"),
	})
	c.Assert(err, gc.IsNil)

	// Retrieve the log from the store.
	var doc mongodoc.Log
	err = store.DB.Logs().Find(nil).One(&doc)
	c.Assert(err, gc.IsNil)

	// The log excludes duplicate URLs.
	c.Assert(doc.URLs, jc.DeepEquals, []*charm.URL{
		charm.MustParseURL("trusty/mysql-42"),
		charm.MustParseURL("mysql"),
	})
}

func (s *StoreSuite) TestCollections(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()
	colls := store.DB.Collections()
	names, err := store.DB.CollectionNames()
	c.Assert(err, gc.IsNil)
	// Some collections don't have indexes so they are created only when used.
	createdOnUse := map[string]bool{
		"migrations": true,
		"macaroons":  true,
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
	// Check that all created collections are mentioned in Collections.
	for _, name := range names {
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
	url := router.MustNewResolvedURL("cs:~charmers/precise/wordpress-23", 23)
	err := store.AddCharmWithArchive(url, wordpress)
	c.Assert(err, gc.IsNil)

	entity, err := store.FindEntity(url, FieldSelector("charmmeta"))
	c.Assert(err, gc.IsNil)
	r, err := store.OpenCachedBlobFile(entity, "", nil)
	c.Assert(err, gc.ErrorMatches, "provided entity does not have required fields")
	c.Assert(r, gc.Equals, nil)
}

func (s *StoreSuite) TestOpenCachedBlobFileWithFoundContent(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()

	wordpress := storetesting.Charms.CharmDir("wordpress")
	url := router.MustNewResolvedURL("cs:~charmers/precise/wordpress-23", 23)
	err := store.AddCharmWithArchive(url, wordpress)
	c.Assert(err, gc.IsNil)

	// Get our expected content.
	data, err := ioutil.ReadFile(filepath.Join(wordpress.Path, "metadata.yaml"))
	c.Assert(err, gc.IsNil)
	expectContent := string(data)

	entity, err := store.FindEntity(url, FieldSelector("blobname", "contents"))
	c.Assert(err, gc.IsNil)

	// Check that, when we open the file for the first time,
	// we see the expected content.
	r, err := store.OpenCachedBlobFile(entity, mongodoc.FileIcon, func(f *zip.File) bool {
		return path.Clean(f.Name) == "metadata.yaml"
	})
	c.Assert(err, gc.IsNil)
	defer r.Close()
	data, err = ioutil.ReadAll(r)
	c.Assert(err, gc.IsNil)
	c.Assert(string(data), gc.Equals, expectContent)

	// When retrieving the entity again, check that the Contents
	// map has been set appropriately...
	entity, err = store.FindEntity(url, FieldSelector("blobname", "contents"))
	c.Assert(err, gc.IsNil)
	c.Assert(entity.Contents, gc.HasLen, 1)
	c.Assert(entity.Contents[mongodoc.FileIcon].IsValid(), gc.Equals, true)

	// ... and that OpenCachedBlobFile still returns a reader with the
	// same data, without making use of the isFile callback.
	r, err = store.OpenCachedBlobFile(entity, mongodoc.FileIcon, func(f *zip.File) bool {
		c.Errorf("isFile called unexpectedly")
		return false
	})
	c.Assert(err, gc.IsNil)
	defer r.Close()
	data, err = ioutil.ReadAll(r)
	c.Assert(err, gc.IsNil)
	c.Assert(string(data), gc.Equals, expectContent)
}

func (s *StoreSuite) TestOpenCachedBlobFileWithNotFoundContent(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()

	wordpress := storetesting.Charms.CharmDir("wordpress")
	url := router.MustNewResolvedURL("cs:~charmers/precise/wordpress-23", 23)
	err := store.AddCharmWithArchive(url, wordpress)
	c.Assert(err, gc.IsNil)

	entity, err := store.FindEntity(url, FieldSelector("blobname", "contents"))
	c.Assert(err, gc.IsNil)

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
	entity, err = store.FindEntity(url, FieldSelector("blobname", "contents"))
	c.Assert(err, gc.IsNil)
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

func (s *StoreSuite) TestSESPutDoesNotErrorWithNoESConfigured(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()
	err := store.UpdateSearch(nil)
	c.Assert(err, gc.IsNil)
}

var findBestEntityCharms = []struct {
	id          *router.ResolvedURL
	charm       charm.Charm
	development bool
	stable      bool
}{{
	id:          router.MustNewResolvedURL("~charmers/trusty/wordpress-0", 0),
	charm:       storetesting.NewCharm(nil),
	development: true,
	stable:      true,
}, {
	id:          router.MustNewResolvedURL("~charmers/trusty/wordpress-1", 1),
	charm:       storetesting.NewCharm(nil),
	development: true,
	stable:      true,
}, {
	id:          router.MustNewResolvedURL("~charmers/trusty/wordpress-2", 2),
	charm:       storetesting.NewCharm(nil),
	development: true,
	stable:      false,
}, {
	id:          router.MustNewResolvedURL("~charmers/trusty/wordpress-3", 3),
	charm:       storetesting.NewCharm(nil),
	development: false,
	stable:      false,
}, {
	id:          router.MustNewResolvedURL("~charmers/precise/wordpress-4", 4),
	charm:       storetesting.NewCharm(nil),
	development: true,
	stable:      true,
}, {
	id:          router.MustNewResolvedURL("~charmers/precise/wordpress-5", 5),
	charm:       storetesting.NewCharm(nil),
	development: true,
	stable:      true,
}, {
	id:          router.MustNewResolvedURL("~charmers/precise/wordpress-6", 6),
	charm:       storetesting.NewCharm(nil),
	development: true,
	stable:      false,
}, {
	id:          router.MustNewResolvedURL("~charmers/precise/wordpress-7", 7),
	charm:       storetesting.NewCharm(nil),
	development: false,
	stable:      false,
}, {
	id:          router.MustNewResolvedURL("~charmers/mysql-0", 0),
	charm:       storetesting.NewCharm(storetesting.MetaWithSupportedSeries(nil, "trusty", "precise")),
	development: true,
	stable:      true,
}, {
	id:          router.MustNewResolvedURL("~charmers/mysql-1", 1),
	charm:       storetesting.NewCharm(storetesting.MetaWithSupportedSeries(nil, "trusty", "precise")),
	development: true,
	stable:      true,
}, {
	id:          router.MustNewResolvedURL("~charmers/mysql-2", 2),
	charm:       storetesting.NewCharm(storetesting.MetaWithSupportedSeries(nil, "trusty", "precise")),
	development: true,
	stable:      false,
}, {
	id:          router.MustNewResolvedURL("~charmers/mysql-3", 3),
	charm:       storetesting.NewCharm(storetesting.MetaWithSupportedSeries(nil, "trusty", "precise")),
	development: false,
	stable:      false,
}, {
	id:          router.MustNewResolvedURL("~charmers/trusty/mongodb-0", -1),
	charm:       storetesting.NewCharm(nil),
	development: true,
	stable:      true,
}, {
	id:          router.MustNewResolvedURL("~charmers/trusty/mongodb-1", -1),
	charm:       storetesting.NewCharm(nil),
	development: true,
	stable:      true,
}, {
	id:          router.MustNewResolvedURL("~charmers/trusty/mongodb-2", -1),
	charm:       storetesting.NewCharm(nil),
	development: true,
	stable:      false,
}, {
	id:          router.MustNewResolvedURL("~charmers/trusty/mongodb-3", -1),
	charm:       storetesting.NewCharm(nil),
	development: false,
	stable:      false,
}, {
	id:          router.MustNewResolvedURL("~charmers/trusty/apache-0", 0),
	charm:       storetesting.NewCharm(nil),
	development: true,
	stable:      false,
}, {
	id:          router.MustNewResolvedURL("~charmers/trusty/nginx-0", 0),
	charm:       storetesting.NewCharm(nil),
	development: false,
	stable:      false,
}, {
	id:          router.MustNewResolvedURL("~charmers/trusty/postgresql-0", 0),
	charm:       storetesting.NewCharm(nil),
	development: true,
	stable:      true,
}, {
	id:          router.MustNewResolvedURL("~charmers/precise/postgresql-0", 0),
	charm:       storetesting.NewCharm(nil),
	development: true,
	stable:      true,
}, {
	id:          router.MustNewResolvedURL("~charmers/postgresql-1", 1),
	charm:       storetesting.NewCharm(storetesting.MetaWithSupportedSeries(nil, "trusty", "precise")),
	development: true,
	stable:      true,
}, {
	id:          router.MustNewResolvedURL("~charmers/trusty/ceph-0", 0),
	charm:       storetesting.NewCharm(nil),
	development: true,
	stable:      true,
}, {
	id:          router.MustNewResolvedURL("~openstack-charmers/trusty/ceph-0", 1),
	charm:       storetesting.NewCharm(nil),
	development: true,
	stable:      false,
}}

var findBestEntityBundles = []struct {
	id          *router.ResolvedURL
	bundle      charm.Bundle
	development bool
	stable      bool
}{{
	id: router.MustNewResolvedURL("~charmers/bundle/wordpress-simple-0", 0),
	bundle: storetesting.NewBundle(&charm.BundleData{
		Applications: map[string]*charm.ApplicationSpec{
			"wordpress": &charm.ApplicationSpec{
				Charm: "cs:wordpress",
			},
			"mysql": &charm.ApplicationSpec{
				Charm: "cs:mysql",
			},
		},
	}),
	development: true,
	stable:      true,
}, {
	id: router.MustNewResolvedURL("~charmers/bundle/wordpress-simple-1", 1),
	bundle: storetesting.NewBundle(&charm.BundleData{
		Applications: map[string]*charm.ApplicationSpec{
			"wordpress": &charm.ApplicationSpec{
				Charm: "cs:wordpress",
			},
			"mysql": &charm.ApplicationSpec{
				Charm: "cs:mysql",
			},
		},
	}),
	development: true,
	stable:      true,
}, {
	id: router.MustNewResolvedURL("~charmers/bundle/wordpress-simple-2", 2),
	bundle: storetesting.NewBundle(&charm.BundleData{
		Applications: map[string]*charm.ApplicationSpec{
			"wordpress": &charm.ApplicationSpec{
				Charm: "cs:wordpress",
			},
			"mysql": &charm.ApplicationSpec{
				Charm: "cs:mysql",
			},
		},
	}),
	development: true,
	stable:      false,
}, {
	id: router.MustNewResolvedURL("~charmers/bundle/wordpress-simple-3", 3),
	bundle: storetesting.NewBundle(&charm.BundleData{
		Applications: map[string]*charm.ApplicationSpec{
			"wordpress": &charm.ApplicationSpec{
				Charm: "cs:wordpress",
			},
			"mysql": &charm.ApplicationSpec{
				Charm: "cs:mysql",
			},
		},
	}),
	development: false,
	stable:      false,
}}

var findBestEntityTests = []struct {
	url              string
	channel          params.Channel
	expectID         *router.ResolvedURL
	expectError      string
	expectErrorCause error
}{{
	url:      "~charmers/trusty/wordpress-0",
	expectID: router.MustNewResolvedURL("~charmers/trusty/wordpress-0", 0),
}, {
	url:      "~charmers/trusty/wordpress-0",
	channel:  params.StableChannel,
	expectID: router.MustNewResolvedURL("~charmers/trusty/wordpress-0", 0),
}, {
	url:      "~charmers/trusty/wordpress-0",
	channel:  params.DevelopmentChannel,
	expectID: router.MustNewResolvedURL("~charmers/trusty/wordpress-0", 0),
}, {
	url:      "~charmers/trusty/wordpress-0",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/trusty/wordpress-0", 0),
}, {
	url:      "~charmers/trusty/wordpress-3",
	expectID: router.MustNewResolvedURL("~charmers/trusty/wordpress-3", 3),
}, {
	url:      "~charmers/trusty/wordpress-3",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/trusty/wordpress-3", 3),
}, {
	url:              "~charmers/trusty/wordpress-3",
	channel:          params.DevelopmentChannel,
	expectError:      "cs:~charmers/trusty/wordpress-3 not found in development channel",
	expectErrorCause: params.ErrNotFound,
}, {
	url:              "~charmers/trusty/wordpress-2",
	channel:          params.StableChannel,
	expectError:      "cs:~charmers/trusty/wordpress-2 not found in stable channel",
	expectErrorCause: params.ErrNotFound,
}, {
	url:      "trusty/wordpress-0",
	expectID: router.MustNewResolvedURL("~charmers/trusty/wordpress-0", 0),
}, {
	url:      "trusty/wordpress-0",
	channel:  params.StableChannel,
	expectID: router.MustNewResolvedURL("~charmers/trusty/wordpress-0", 0),
}, {
	url:      "trusty/wordpress-0",
	channel:  params.DevelopmentChannel,
	expectID: router.MustNewResolvedURL("~charmers/trusty/wordpress-0", 0),
}, {
	url:      "trusty/wordpress-0",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/trusty/wordpress-0", 0),
}, {
	url:      "trusty/wordpress-3",
	expectID: router.MustNewResolvedURL("~charmers/trusty/wordpress-3", 3),
}, {
	url:      "trusty/wordpress-3",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/trusty/wordpress-3", 3),
}, {
	url:              "trusty/wordpress-3",
	channel:          params.DevelopmentChannel,
	expectError:      "cs:trusty/wordpress-3 not found in development channel",
	expectErrorCause: params.ErrNotFound,
}, {
	url:              "trusty/wordpress-2",
	channel:          params.StableChannel,
	expectError:      "cs:trusty/wordpress-2 not found in stable channel",
	expectErrorCause: params.ErrNotFound,
}, {
	url:      "~charmers/trusty/wordpress",
	expectID: router.MustNewResolvedURL("~charmers/trusty/wordpress-1", 1),
}, {
	url:      "~charmers/trusty/wordpress",
	channel:  params.StableChannel,
	expectID: router.MustNewResolvedURL("~charmers/trusty/wordpress-1", 1),
}, {
	url:      "~charmers/trusty/wordpress",
	channel:  params.DevelopmentChannel,
	expectID: router.MustNewResolvedURL("~charmers/trusty/wordpress-2", 2),
}, {
	url:      "~charmers/trusty/wordpress",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/trusty/wordpress-3", 3),
}, {
	url:      "trusty/wordpress",
	expectID: router.MustNewResolvedURL("~charmers/trusty/wordpress-1", 1),
}, {
	url:      "trusty/wordpress",
	channel:  params.StableChannel,
	expectID: router.MustNewResolvedURL("~charmers/trusty/wordpress-1", 1),
}, {
	url:      "trusty/wordpress",
	channel:  params.DevelopmentChannel,
	expectID: router.MustNewResolvedURL("~charmers/trusty/wordpress-2", 2),
}, {
	url:      "trusty/wordpress",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/trusty/wordpress-3", 3),
}, {
	url:      "precise/wordpress",
	expectID: router.MustNewResolvedURL("~charmers/precise/wordpress-5", 5),
}, {
	url:      "precise/wordpress",
	channel:  params.StableChannel,
	expectID: router.MustNewResolvedURL("~charmers/precise/wordpress-5", 5),
}, {
	url:      "precise/wordpress",
	channel:  params.DevelopmentChannel,
	expectID: router.MustNewResolvedURL("~charmers/precise/wordpress-6", 6),
}, {
	url:      "precise/wordpress",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/precise/wordpress-7", 7),
}, {
	url:      "wordpress",
	expectID: router.MustNewResolvedURL("~charmers/trusty/wordpress-1", 1),
}, {
	url:      "wordpress",
	channel:  params.StableChannel,
	expectID: router.MustNewResolvedURL("~charmers/trusty/wordpress-1", 1),
}, {
	url:      "wordpress",
	channel:  params.DevelopmentChannel,
	expectID: router.MustNewResolvedURL("~charmers/trusty/wordpress-2", 2),
}, {
	url:      "wordpress",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/trusty/wordpress-3", 3),
}, {
	url:      "~charmers/wordpress",
	expectID: router.MustNewResolvedURL("~charmers/trusty/wordpress-1", 1),
}, {
	url:      "~charmers/wordpress",
	channel:  params.StableChannel,
	expectID: router.MustNewResolvedURL("~charmers/trusty/wordpress-1", 1),
}, {
	url:      "~charmers/wordpress",
	channel:  params.DevelopmentChannel,
	expectID: router.MustNewResolvedURL("~charmers/trusty/wordpress-2", 2),
}, {
	url:      "~charmers/wordpress",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/trusty/wordpress-3", 3),
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
	channel:          params.DevelopmentChannel,
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
	channel:  params.DevelopmentChannel,
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
	channel:          params.DevelopmentChannel,
	expectError:      "cs:~charmers/mysql-3 not found in development channel",
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
	channel:  params.DevelopmentChannel,
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
	channel:          params.DevelopmentChannel,
	expectError:      "cs:mysql-3 not found in development channel",
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
	channel:  params.DevelopmentChannel,
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
	channel:  params.DevelopmentChannel,
	expectID: router.MustNewResolvedURL("~charmers/mysql-2", 2),
}, {
	url:      "mysql",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/mysql-3", 3),
}, {
	url:      "~charmers/precise/mysql",
	expectID: router.MustNewResolvedURL("~charmers/mysql-1", 1),
}, {
	url:      "~charmers/precise/mysql",
	channel:  params.StableChannel,
	expectID: router.MustNewResolvedURL("~charmers/mysql-1", 1),
}, {
	url:      "~charmers/precise/mysql",
	channel:  params.DevelopmentChannel,
	expectID: router.MustNewResolvedURL("~charmers/mysql-2", 2),
}, {
	url:      "~charmers/precise/mysql",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/mysql-3", 3),
}, {
	url:      "precise/mysql",
	expectID: router.MustNewResolvedURL("~charmers/mysql-1", 1),
}, {
	url:      "precise/mysql",
	channel:  params.StableChannel,
	expectID: router.MustNewResolvedURL("~charmers/mysql-1", 1),
}, {
	url:      "precise/mysql",
	channel:  params.DevelopmentChannel,
	expectID: router.MustNewResolvedURL("~charmers/mysql-2", 2),
}, {
	url:      "precise/mysql",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/mysql-3", 3),
}, {
	url:      "~charmers/trusty/mysql",
	expectID: router.MustNewResolvedURL("~charmers/mysql-1", 1),
}, {
	url:      "~charmers/trusty/mysql",
	channel:  params.StableChannel,
	expectID: router.MustNewResolvedURL("~charmers/mysql-1", 1),
}, {
	url:      "~charmers/trusty/mysql",
	channel:  params.DevelopmentChannel,
	expectID: router.MustNewResolvedURL("~charmers/mysql-2", 2),
}, {
	url:      "~charmers/trusty/mysql",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/mysql-3", 3),
}, {
	url:      "trusty/mysql",
	expectID: router.MustNewResolvedURL("~charmers/mysql-1", 1),
}, {
	url:      "trusty/mysql",
	channel:  params.StableChannel,
	expectID: router.MustNewResolvedURL("~charmers/mysql-1", 1),
}, {
	url:      "trusty/mysql",
	channel:  params.DevelopmentChannel,
	expectID: router.MustNewResolvedURL("~charmers/mysql-2", 2),
}, {
	url:      "trusty/mysql",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/mysql-3", 3),
}, {
	url:      "~charmers/trusty/mongodb-0",
	expectID: router.MustNewResolvedURL("~charmers/trusty/mongodb-0", -1),
}, {
	url:      "~charmers/trusty/mongodb-0",
	channel:  params.StableChannel,
	expectID: router.MustNewResolvedURL("~charmers/trusty/mongodb-0", -1),
}, {
	url:      "~charmers/trusty/mongodb-0",
	channel:  params.DevelopmentChannel,
	expectID: router.MustNewResolvedURL("~charmers/trusty/mongodb-0", -1),
}, {
	url:      "~charmers/trusty/mongodb-0",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/trusty/mongodb-0", -1),
}, {
	url:      "~charmers/trusty/mongodb-3",
	expectID: router.MustNewResolvedURL("~charmers/trusty/mongodb-3", -1),
}, {
	url:      "~charmers/trusty/mongodb-3",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/trusty/mongodb-3", -1),
}, {
	url:              "~charmers/trusty/mongodb-3",
	channel:          params.DevelopmentChannel,
	expectError:      "cs:~charmers/trusty/mongodb-3 not found in development channel",
	expectErrorCause: params.ErrNotFound,
}, {
	url:              "~charmers/trusty/mongodb-2",
	channel:          params.StableChannel,
	expectError:      "cs:~charmers/trusty/mongodb-2 not found in stable channel",
	expectErrorCause: params.ErrNotFound,
}, {
	url:              "trusty/mongodb-0",
	expectError:      "no matching charm or bundle for cs:trusty/mongodb-0",
	expectErrorCause: params.ErrNotFound,
}, {
	url:              "trusty/mongodb-0",
	channel:          params.StableChannel,
	expectError:      "no matching charm or bundle for cs:trusty/mongodb-0",
	expectErrorCause: params.ErrNotFound,
}, {
	url:              "trusty/mongodb-0",
	channel:          params.DevelopmentChannel,
	expectError:      "no matching charm or bundle for cs:trusty/mongodb-0",
	expectErrorCause: params.ErrNotFound,
}, {
	url:              "trusty/mongodb-0",
	channel:          params.UnpublishedChannel,
	expectError:      "no matching charm or bundle for cs:trusty/mongodb-0",
	expectErrorCause: params.ErrNotFound,
}, {
	url:      "~charmers/trusty/mongodb",
	expectID: router.MustNewResolvedURL("~charmers/trusty/mongodb-1", -1),
}, {
	url:      "~charmers/trusty/mongodb",
	channel:  params.StableChannel,
	expectID: router.MustNewResolvedURL("~charmers/trusty/mongodb-1", -1),
}, {
	url:      "~charmers/trusty/mongodb",
	channel:  params.DevelopmentChannel,
	expectID: router.MustNewResolvedURL("~charmers/trusty/mongodb-2", -1),
}, {
	url:      "~charmers/trusty/mongodb",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/trusty/mongodb-3", -1),
}, {
	url:              "trusty/mongodb",
	expectError:      "no matching charm or bundle for cs:trusty/mongodb",
	expectErrorCause: params.ErrNotFound,
}, {
	url:              "trusty/mongodb",
	channel:          params.StableChannel,
	expectError:      "no matching charm or bundle for cs:trusty/mongodb",
	expectErrorCause: params.ErrNotFound,
}, {
	url:              "trusty/mongodb",
	channel:          params.DevelopmentChannel,
	expectError:      "no matching charm or bundle for cs:trusty/mongodb",
	expectErrorCause: params.ErrNotFound,
}, {
	url:              "trusty/mongodb",
	channel:          params.UnpublishedChannel,
	expectError:      "no matching charm or bundle for cs:trusty/mongodb",
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
	channel:          params.DevelopmentChannel,
	expectError:      "no matching charm or bundle for cs:mongodb",
	expectErrorCause: params.ErrNotFound,
}, {
	url:              "mongodb",
	channel:          params.UnpublishedChannel,
	expectError:      "no matching charm or bundle for cs:mongodb",
	expectErrorCause: params.ErrNotFound,
}, {
	url:              "~charmers/trusty/apache",
	expectError:      "no matching charm or bundle for cs:~charmers/trusty/apache",
	expectErrorCause: params.ErrNotFound,
}, {
	url:              "~charmers/trusty/apache",
	channel:          params.StableChannel,
	expectError:      "no matching charm or bundle for cs:~charmers/trusty/apache",
	expectErrorCause: params.ErrNotFound,
}, {
	url:      "~charmers/trusty/apache",
	channel:  params.DevelopmentChannel,
	expectID: router.MustNewResolvedURL("~charmers/trusty/apache-0", 0),
}, {
	url:      "~charmers/trusty/apache",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/trusty/apache-0", 0),
}, {
	url:      "~charmers/trusty/apache-0",
	expectID: router.MustNewResolvedURL("~charmers/trusty/apache-0", 0),
}, {
	url:         "~charmers/trusty/apache-0",
	channel:     params.StableChannel,
	expectError: "cs:~charmers/trusty/apache-0 not found in stable channel",
}, {
	url:      "~charmers/trusty/apache-0",
	channel:  params.DevelopmentChannel,
	expectID: router.MustNewResolvedURL("~charmers/trusty/apache-0", 0),
}, {
	url:      "~charmers/trusty/apache-0",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/trusty/apache-0", 0),
}, {
	url:              "trusty/apache",
	expectError:      "no matching charm or bundle for cs:trusty/apache",
	expectErrorCause: params.ErrNotFound,
}, {
	url:              "trusty/apache",
	channel:          params.StableChannel,
	expectError:      "no matching charm or bundle for cs:trusty/apache",
	expectErrorCause: params.ErrNotFound,
}, {
	url:      "trusty/apache",
	channel:  params.DevelopmentChannel,
	expectID: router.MustNewResolvedURL("~charmers/trusty/apache-0", 0),
}, {
	url:      "trusty/apache",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/trusty/apache-0", 0),
}, {
	url:      "trusty/apache-0",
	expectID: router.MustNewResolvedURL("~charmers/trusty/apache-0", 0),
}, {
	url:         "trusty/apache-0",
	channel:     params.StableChannel,
	expectError: "cs:trusty/apache-0 not found in stable channel",
}, {
	url:      "trusty/apache-0",
	channel:  params.DevelopmentChannel,
	expectID: router.MustNewResolvedURL("~charmers/trusty/apache-0", 0),
}, {
	url:      "trusty/apache-0",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/trusty/apache-0", 0),
}, {
	url:              "~charmers/trusty/nginx",
	expectError:      "no matching charm or bundle for cs:~charmers/trusty/nginx",
	expectErrorCause: params.ErrNotFound,
}, {
	url:              "~charmers/trusty/nginx",
	channel:          params.StableChannel,
	expectError:      "no matching charm or bundle for cs:~charmers/trusty/nginx",
	expectErrorCause: params.ErrNotFound,
}, {
	url:              "~charmers/trusty/nginx",
	channel:          params.DevelopmentChannel,
	expectError:      "no matching charm or bundle for cs:~charmers/trusty/nginx",
	expectErrorCause: params.ErrNotFound,
}, {
	url:      "~charmers/trusty/nginx",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/trusty/nginx-0", 0),
}, {
	url:      "~charmers/trusty/nginx-0",
	expectID: router.MustNewResolvedURL("~charmers/trusty/nginx-0", 0),
}, {
	url:         "~charmers/trusty/nginx-0",
	channel:     params.StableChannel,
	expectError: "cs:~charmers/trusty/nginx-0 not found in stable channel",
}, {
	url:         "~charmers/trusty/nginx-0",
	channel:     params.DevelopmentChannel,
	expectError: "cs:~charmers/trusty/nginx-0 not found in development channel",
}, {
	url:      "~charmers/trusty/nginx-0",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/trusty/nginx-0", 0),
}, {
	url:              "trusty/nginx",
	expectError:      "no matching charm or bundle for cs:trusty/nginx",
	expectErrorCause: params.ErrNotFound,
}, {
	url:              "trusty/nginx",
	channel:          params.StableChannel,
	expectError:      "no matching charm or bundle for cs:trusty/nginx",
	expectErrorCause: params.ErrNotFound,
}, {
	url:              "trusty/nginx",
	channel:          params.DevelopmentChannel,
	expectError:      "no matching charm or bundle for cs:trusty/nginx",
	expectErrorCause: params.ErrNotFound,
}, {
	url:      "trusty/nginx",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/trusty/nginx-0", 0),
}, {
	url:      "trusty/nginx-0",
	expectID: router.MustNewResolvedURL("~charmers/trusty/nginx-0", 0),
}, {
	url:         "trusty/nginx-0",
	channel:     params.StableChannel,
	expectError: "cs:trusty/nginx-0 not found in stable channel",
}, {
	url:         "trusty/nginx-0",
	channel:     params.DevelopmentChannel,
	expectError: "cs:trusty/nginx-0 not found in development channel",
}, {
	url:      "trusty/nginx-0",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/trusty/nginx-0", 0),
}, {
	url:      "~charmers/bundle/wordpress-simple-0",
	expectID: router.MustNewResolvedURL("~charmers/bundle/wordpress-simple-0", 0),
}, {
	url:      "~charmers/bundle/wordpress-simple-0",
	channel:  params.StableChannel,
	expectID: router.MustNewResolvedURL("~charmers/bundle/wordpress-simple-0", 0),
}, {
	url:      "~charmers/bundle/wordpress-simple-0",
	channel:  params.DevelopmentChannel,
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
	channel:     params.DevelopmentChannel,
	expectError: "cs:~charmers/bundle/wordpress-simple-3 not found in development channel",
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
	channel:  params.DevelopmentChannel,
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
	channel:     params.DevelopmentChannel,
	expectError: "cs:bundle/wordpress-simple-3 not found in development channel",
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
	channel:  params.DevelopmentChannel,
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
	channel:  params.DevelopmentChannel,
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
	channel:  params.DevelopmentChannel,
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
	channel:  params.DevelopmentChannel,
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
	channel:  params.DevelopmentChannel,
	expectID: router.MustNewResolvedURL("~charmers/bundle/wordpress-simple-0", 0),
}, {
	url:      "~charmers/wordpress-simple-0",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/bundle/wordpress-simple-0", 0),
}, {
	url:              "~charmers/trusty/wordpress",
	channel:          "no-such-channel",
	expectError:      "no matching charm or bundle for cs:~charmers/trusty/wordpress",
	expectErrorCause: params.ErrNotFound,
}, {
	url:      "~charmers/trusty/postgresql-0",
	expectID: router.MustNewResolvedURL("~charmers/trusty/postgresql-0", 0),
}, {
	url:      "~charmers/trusty/postgresql-0",
	channel:  params.StableChannel,
	expectID: router.MustNewResolvedURL("~charmers/trusty/postgresql-0", 0),
}, {
	url:      "~charmers/trusty/postgresql-0",
	channel:  params.DevelopmentChannel,
	expectID: router.MustNewResolvedURL("~charmers/trusty/postgresql-0", 0),
}, {
	url:      "~charmers/trusty/postgresql-0",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/trusty/postgresql-0", 0),
}, {
	url:      "~charmers/precise/postgresql-0",
	expectID: router.MustNewResolvedURL("~charmers/precise/postgresql-0", 0),
}, {
	url:      "~charmers/precise/postgresql-0",
	channel:  params.StableChannel,
	expectID: router.MustNewResolvedURL("~charmers/precise/postgresql-0", 0),
}, {
	url:      "~charmers/precise/postgresql-0",
	channel:  params.DevelopmentChannel,
	expectID: router.MustNewResolvedURL("~charmers/precise/postgresql-0", 0),
}, {
	url:      "~charmers/precise/postgresql-0",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/precise/postgresql-0", 0),
}, {
	url:      "~charmers/trusty/postgresql-1",
	expectID: router.MustNewResolvedURL("~charmers/postgresql-1", 1),
}, {
	url:      "~charmers/trusty/postgresql-1",
	channel:  params.StableChannel,
	expectID: router.MustNewResolvedURL("~charmers/postgresql-1", 1),
}, {
	url:      "~charmers/trusty/postgresql-1",
	channel:  params.DevelopmentChannel,
	expectID: router.MustNewResolvedURL("~charmers/postgresql-1", 1),
}, {
	url:      "~charmers/trusty/postgresql-1",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/postgresql-1", 1),
}, {
	url:      "~charmers/precise/postgresql-1",
	expectID: router.MustNewResolvedURL("~charmers/postgresql-1", 1),
}, {
	url:      "~charmers/precise/postgresql-1",
	channel:  params.StableChannel,
	expectID: router.MustNewResolvedURL("~charmers/postgresql-1", 1),
}, {
	url:      "~charmers/precise/postgresql-1",
	channel:  params.DevelopmentChannel,
	expectID: router.MustNewResolvedURL("~charmers/postgresql-1", 1),
}, {
	url:      "~charmers/precise/postgresql-1",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/postgresql-1", 1),
}, {
	url:      "~charmers/trusty/postgresql",
	expectID: router.MustNewResolvedURL("~charmers/postgresql-1", 1),
}, {
	url:      "~charmers/trusty/postgresql",
	channel:  params.StableChannel,
	expectID: router.MustNewResolvedURL("~charmers/postgresql-1", 1),
}, {
	url:      "~charmers/trusty/postgresql",
	channel:  params.DevelopmentChannel,
	expectID: router.MustNewResolvedURL("~charmers/postgresql-1", 1),
}, {
	url:      "~charmers/trusty/postgresql",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/postgresql-1", 1),
}, {
	url:      "~charmers/precise/postgresql",
	expectID: router.MustNewResolvedURL("~charmers/postgresql-1", 1),
}, {
	url:      "~charmers/precise/postgresql",
	channel:  params.StableChannel,
	expectID: router.MustNewResolvedURL("~charmers/postgresql-1", 1),
}, {
	url:      "~charmers/precise/postgresql",
	channel:  params.DevelopmentChannel,
	expectID: router.MustNewResolvedURL("~charmers/postgresql-1", 1),
}, {
	url:      "~charmers/precise/postgresql",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/postgresql-1", 1),
}, {
	url:      "trusty/postgresql",
	expectID: router.MustNewResolvedURL("~charmers/postgresql-1", 1),
}, {
	url:      "trusty/postgresql",
	channel:  params.StableChannel,
	expectID: router.MustNewResolvedURL("~charmers/postgresql-1", 1),
}, {
	url:      "trusty/postgresql",
	channel:  params.DevelopmentChannel,
	expectID: router.MustNewResolvedURL("~charmers/postgresql-1", 1),
}, {
	url:      "trusty/postgresql",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/postgresql-1", 1),
}, {
	url:      "precise/postgresql",
	expectID: router.MustNewResolvedURL("~charmers/postgresql-1", 1),
}, {
	url:      "precise/postgresql",
	channel:  params.StableChannel,
	expectID: router.MustNewResolvedURL("~charmers/postgresql-1", 1),
}, {
	url:      "precise/postgresql",
	channel:  params.DevelopmentChannel,
	expectID: router.MustNewResolvedURL("~charmers/postgresql-1", 1),
}, {
	url:      "precise/postgresql",
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
	channel:  params.DevelopmentChannel,
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
	channel:  params.DevelopmentChannel,
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
	channel:          params.DevelopmentChannel,
	expectError:      "no matching charm or bundle for cs:postgresql-0",
	expectErrorCause: params.ErrNotFound,
}, {
	url:              "postgresql-0",
	channel:          params.UnpublishedChannel,
	expectError:      "no matching charm or bundle for cs:postgresql-0",
	expectErrorCause: params.ErrNotFound,
}, {
	url:      "~charmers/trusty/ceph-0",
	expectID: router.MustNewResolvedURL("~charmers/trusty/ceph-0", 0),
}, {
	url:      "~charmers/trusty/ceph-0",
	channel:  params.StableChannel,
	expectID: router.MustNewResolvedURL("~charmers/trusty/ceph-0", 0),
}, {
	url:      "~charmers/trusty/ceph-0",
	channel:  params.DevelopmentChannel,
	expectID: router.MustNewResolvedURL("~charmers/trusty/ceph-0", 0),
}, {
	url:      "~charmers/trusty/ceph-0",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/trusty/ceph-0", 0),
}, {
	url:      "~charmers/trusty/ceph",
	expectID: router.MustNewResolvedURL("~charmers/trusty/ceph-0", 0),
}, {
	url:      "~charmers/trusty/ceph",
	channel:  params.StableChannel,
	expectID: router.MustNewResolvedURL("~charmers/trusty/ceph-0", 0),
}, {
	url:      "~charmers/trusty/ceph",
	channel:  params.DevelopmentChannel,
	expectID: router.MustNewResolvedURL("~charmers/trusty/ceph-0", 0),
}, {
	url:      "~charmers/trusty/ceph",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/trusty/ceph-0", 0),
}, {
	url:      "~openstack-charmers/trusty/ceph-0",
	expectID: router.MustNewResolvedURL("~openstack-charmers/trusty/ceph-0", 1),
}, {
	url:         "~openstack-charmers/trusty/ceph-0",
	channel:     params.StableChannel,
	expectError: "cs:~openstack-charmers/trusty/ceph-0 not found in stable channel",
}, {
	url:      "~openstack-charmers/trusty/ceph-0",
	channel:  params.DevelopmentChannel,
	expectID: router.MustNewResolvedURL("~openstack-charmers/trusty/ceph-0", 1),
}, {
	url:      "~openstack-charmers/trusty/ceph-0",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~openstack-charmers/trusty/ceph-0", 1),
}, {
	url:              "~openstack-charmers/trusty/ceph",
	expectError:      "no matching charm or bundle for cs:~openstack-charmers/trusty/ceph",
	expectErrorCause: params.ErrNotFound,
}, {
	url:              "~openstack-charmers/trusty/ceph",
	channel:          params.StableChannel,
	expectError:      "no matching charm or bundle for cs:~openstack-charmers/trusty/ceph",
	expectErrorCause: params.ErrNotFound,
}, {
	url:      "~openstack-charmers/trusty/ceph",
	channel:  params.DevelopmentChannel,
	expectID: router.MustNewResolvedURL("~openstack-charmers/trusty/ceph-0", 1),
}, {
	url:      "~openstack-charmers/trusty/ceph",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~openstack-charmers/trusty/ceph-0", 1),
}, {
	url:      "trusty/ceph-0",
	expectID: router.MustNewResolvedURL("~charmers/trusty/ceph-0", 0),
}, {
	url:      "trusty/ceph-0",
	channel:  params.StableChannel,
	expectID: router.MustNewResolvedURL("~charmers/trusty/ceph-0", 0),
}, {
	url:      "trusty/ceph-0",
	channel:  params.DevelopmentChannel,
	expectID: router.MustNewResolvedURL("~charmers/trusty/ceph-0", 0),
}, {
	url:      "trusty/ceph-0",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~charmers/trusty/ceph-0", 0),
}, {
	url:      "trusty/ceph-1",
	expectID: router.MustNewResolvedURL("~openstack-charmers/trusty/ceph-0", 1),
}, {
	url:         "trusty/ceph-1",
	channel:     params.StableChannel,
	expectError: "cs:trusty/ceph-1 not found in stable channel",
}, {
	url:      "trusty/ceph-1",
	channel:  params.DevelopmentChannel,
	expectID: router.MustNewResolvedURL("~openstack-charmers/trusty/ceph-0", 1),
}, {
	url:      "trusty/ceph-1",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~openstack-charmers/trusty/ceph-0", 1),
}, {
	url:              "trusty/ceph",
	expectError:      "no matching charm or bundle for cs:trusty/ceph",
	expectErrorCause: params.ErrNotFound,
}, {
	url:              "trusty/ceph",
	channel:          params.StableChannel,
	expectError:      "no matching charm or bundle for cs:trusty/ceph",
	expectErrorCause: params.ErrNotFound,
}, {
	url:      "trusty/ceph",
	channel:  params.DevelopmentChannel,
	expectID: router.MustNewResolvedURL("~openstack-charmers/trusty/ceph-0", 1),
}, {
	url:      "trusty/ceph",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~openstack-charmers/trusty/ceph-0", 1),
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
	channel:  params.DevelopmentChannel,
	expectID: router.MustNewResolvedURL("~openstack-charmers/trusty/ceph-0", 1),
}, {
	url:      "ceph",
	channel:  params.UnpublishedChannel,
	expectID: router.MustNewResolvedURL("~openstack-charmers/trusty/ceph-0", 1),
}}

func (s *StoreSuite) TestFindBestEntity(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()
	for _, ch := range findBestEntityCharms {
		err := store.AddCharmWithArchive(ch.id, ch.charm)
		c.Assert(err, gc.IsNil)
		err = store.SetPromulgated(ch.id, ch.id.PromulgatedRevision != -1)
		c.Assert(err, gc.IsNil)
		if ch.development {
			err := store.Publish(ch.id, nil, params.DevelopmentChannel)
			c.Assert(err, gc.IsNil)
		}
		if ch.stable {
			err := store.Publish(ch.id, nil, params.StableChannel)
			c.Assert(err, gc.IsNil)
		}
	}

	for _, b := range findBestEntityBundles {
		err := store.AddBundleWithArchive(b.id, b.bundle)
		c.Assert(err, gc.IsNil)
		err = store.SetPromulgated(b.id, b.id.PromulgatedRevision != -1)
		c.Assert(err, gc.IsNil)
		if b.development {
			err := store.Publish(b.id, nil, params.DevelopmentChannel)
			c.Assert(err, gc.IsNil)
		}
		if b.stable {
			err := store.Publish(b.id, nil, params.StableChannel)
			c.Assert(err, gc.IsNil)
		}
	}

	for i, test := range findBestEntityTests {
		c.Logf("test %d: %s (%s)", i, test.url, test.channel)
		entity, err := store.FindBestEntity(charm.MustParseURL(test.url), test.channel, nil)
		if test.expectError != "" {
			c.Assert(err, gc.ErrorMatches, test.expectError)
			if test.expectErrorCause != nil {
				c.Assert(errgo.Cause(err), gc.Equals, test.expectErrorCause)
			}
			continue
		}
		c.Assert(err, gc.IsNil)
		c.Assert(EntityResolvedURL(entity), jc.DeepEquals, test.expectID)
	}
}

var matchingInterfacesQueryTests = []struct {
	required []string
	provided []string
	expect   []string
}{{
	provided: []string{"a"},
	expect: []string{
		"cs:~charmers/trusty/wordpress-1",
		"cs:~charmers/trusty/wordpress-2",
	},
}, {
	provided: []string{"a", "b", "d"},
	required: []string{"b", "c", "e"},
	expect: []string{
		"cs:~charmers/trusty/mysql-1",
		"cs:~charmers/trusty/wordpress-1",
		"cs:~charmers/trusty/wordpress-2",
	},
}, {
	required: []string{"x"},
	expect: []string{
		"cs:~charmers/trusty/mysql-1",
		"cs:~charmers/trusty/wordpress-2",
	},
}, {
	expect: []string{},
}}

func (s *StoreSuite) TestMatchingInterfacesQuery(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()
	entities := []*mongodoc.Entity{{
		URL:                     charm.MustParseURL("~charmers/trusty/wordpress-1"),
		PromulgatedURL:          charm.MustParseURL("trusty/wordpress-1"),
		CharmProvidedInterfaces: []string{"a", "b"},
		CharmRequiredInterfaces: []string{"b", "c"},
	}, {
		URL:                     charm.MustParseURL("~charmers/trusty/wordpress-2"),
		PromulgatedURL:          charm.MustParseURL("trusty/wordpress-2"),
		CharmProvidedInterfaces: []string{"a", "b"},
		CharmRequiredInterfaces: []string{"b", "c", "x"},
	}, {
		URL:                     charm.MustParseURL("~charmers/trusty/mysql-1"),
		PromulgatedURL:          charm.MustParseURL("trusty/mysql-1"),
		CharmProvidedInterfaces: []string{"d", "b"},
		CharmRequiredInterfaces: []string{"e", "x"},
	}}
	for _, e := range entities {
		err := store.DB.Entities().Insert(denormalizedEntity(e))
		c.Assert(err, gc.IsNil)
	}
	for i, test := range matchingInterfacesQueryTests {
		c.Logf("test %d: req %v; prov %v", i, test.required, test.provided)
		var entities []*mongodoc.Entity
		err := store.MatchingInterfacesQuery(test.required, test.provided).All(&entities)
		c.Assert(err, gc.IsNil)
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
	url: "~charmers/trusty/wordpress-10",
}, {
	url:       "~charmers/precise/wordpress-10",
	expectErr: `cannot update "cs:precise/wordpress-10": not found`,
}}

func (s *StoreSuite) TestUpdateEntity(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()
	for i, test := range updateEntityTests {
		c.Logf("test %d. %s", i, test.url)
		url := router.MustNewResolvedURL(test.url, 10)
		_, err := store.DB.Entities().RemoveAll(nil)
		c.Assert(err, gc.IsNil)
		err = store.DB.Entities().Insert(denormalizedEntity(&mongodoc.Entity{
			URL:            charm.MustParseURL("~charmers/trusty/wordpress-10"),
			PromulgatedURL: charm.MustParseURL("trusty/wordpress-4"),
		}))
		c.Assert(err, gc.IsNil)
		err = store.UpdateEntity(url, bson.D{{"$set", bson.D{{"extrainfo.test", []byte("PASS")}}}})
		if test.expectErr != "" {
			c.Assert(err, gc.ErrorMatches, test.expectErr)
		} else {
			c.Assert(err, gc.IsNil)
			entity, err := store.FindEntity(url, nil)
			c.Assert(err, gc.IsNil)
			c.Assert(string(entity.ExtraInfo["test"]), gc.Equals, "PASS")
		}
	}
}

var updateBaseEntityTests = []struct {
	url       string
	expectErr string
}{{
	url: "~charmers/trusty/wordpress-10",
}, {
	url:       "~charmers/precise/mysql-10",
	expectErr: `cannot update base entity for "cs:precise/mysql-10": not found`,
}}

func (s *StoreSuite) TestUpdateBaseEntity(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()
	for i, test := range updateBaseEntityTests {
		c.Logf("test %d. %s", i, test.url)
		url := router.MustNewResolvedURL(test.url, 10)
		_, err := store.DB.BaseEntities().RemoveAll(nil)
		c.Assert(err, gc.IsNil)
		err = store.DB.BaseEntities().Insert(&mongodoc.BaseEntity{
			URL:         charm.MustParseURL("~charmers/wordpress"),
			User:        "charmers",
			Name:        "wordpress",
			Promulgated: true,
		})
		c.Assert(err, gc.IsNil)
		err = store.UpdateBaseEntity(url, bson.D{{"$set", bson.D{{"channelacls.unpublished", mongodoc.ACL{
			Read: []string{"test"},
		}}}}})
		if test.expectErr != "" {
			c.Assert(err, gc.ErrorMatches, test.expectErr)
		} else {
			c.Assert(err, gc.IsNil)
			baseEntity, err := store.FindBaseEntity(&url.URL, nil)
			c.Assert(err, gc.IsNil)
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
		entity("~charmers/trusty/wordpress-0", ""),
	},
	baseEntities: []*mongodoc.BaseEntity{
		baseEntity("~charmers/wordpress", false),
	},
	url:        "~charmers/trusty/wordpress-0",
	promulgate: true,
	expectEntities: []*mongodoc.Entity{
		entity("~charmers/trusty/wordpress-0", "trusty/wordpress-0"),
	},
	expectBaseEntities: []*mongodoc.BaseEntity{
		baseEntity("~charmers/wordpress", true),
	},
}, {
	about: "multiple series not already promulgated",
	entities: []*mongodoc.Entity{
		entity("~charmers/trusty/wordpress-0", ""),
		entity("~charmers/precise/wordpress-0", ""),
	},
	baseEntities: []*mongodoc.BaseEntity{
		baseEntity("~charmers/wordpress", false),
	},
	url:        "~charmers/trusty/wordpress-0",
	promulgate: true,
	expectEntities: []*mongodoc.Entity{
		entity("~charmers/trusty/wordpress-0", "trusty/wordpress-0"),
		entity("~charmers/precise/wordpress-0", "precise/wordpress-0"),
	},
	expectBaseEntities: []*mongodoc.BaseEntity{
		baseEntity("~charmers/wordpress", true),
	},
}, {
	about: "charm promulgated as different user",
	entities: []*mongodoc.Entity{
		entity("~charmers/trusty/wordpress-0", "trusty/wordpress-0"),
		entity("~test-charmers/trusty/wordpress-0", ""),
	},
	baseEntities: []*mongodoc.BaseEntity{
		baseEntity("~charmers/wordpress", true),
		baseEntity("~test-charmers/wordpress", false),
	},
	url:        "~test-charmers/trusty/wordpress-0",
	promulgate: true,
	expectEntities: []*mongodoc.Entity{
		entity("~charmers/trusty/wordpress-0", "trusty/wordpress-0"),
		entity("~test-charmers/trusty/wordpress-0", "trusty/wordpress-1"),
	},
	expectBaseEntities: []*mongodoc.BaseEntity{
		baseEntity("~charmers/wordpress", false),
		baseEntity("~test-charmers/wordpress", true),
	},
}, {
	about: "single charm already promulgated",
	entities: []*mongodoc.Entity{
		entity("~charmers/trusty/wordpress-0", "trusty/wordpress-0"),
	},
	baseEntities: []*mongodoc.BaseEntity{
		baseEntity("~charmers/wordpress", true),
	},
	url:        "~charmers/trusty/wordpress-0",
	promulgate: true,
	expectEntities: []*mongodoc.Entity{
		entity("~charmers/trusty/wordpress-0", "trusty/wordpress-0"),
	},
	expectBaseEntities: []*mongodoc.BaseEntity{
		baseEntity("~charmers/wordpress", true),
	},
}, {
	about: "unrelated charms are unaffected",
	entities: []*mongodoc.Entity{
		entity("~charmers/trusty/wordpress-0", ""),
		entity("~test-charmers/trusty/mysql-0", "trusty/mysql-0"),
	},
	baseEntities: []*mongodoc.BaseEntity{
		baseEntity("~charmers/wordpress", false),
		baseEntity("~test-charmers/mysql", true),
	},
	url:        "~charmers/trusty/wordpress-0",
	promulgate: true,
	expectEntities: []*mongodoc.Entity{
		entity("~charmers/trusty/wordpress-0", "trusty/wordpress-0"),
		entity("~test-charmers/trusty/mysql-0", "trusty/mysql-0"),
	},
	expectBaseEntities: []*mongodoc.BaseEntity{
		baseEntity("~charmers/wordpress", true),
		baseEntity("~test-charmers/mysql", true),
	},
}, {
	about: "only one owner promulgated",
	entities: []*mongodoc.Entity{
		entity("~charmers/trusty/wordpress-0", ""),
		entity("~test-charmers/trusty/wordpress-0", "trusty/wordpress-0"),
		entity("~test2-charmers/trusty/wordpress-0", "trusty/wordpress-1"),
	},
	baseEntities: []*mongodoc.BaseEntity{
		baseEntity("~charmers/wordpress", false),
		baseEntity("~test-charmers/wordpress", false),
		baseEntity("~test2-charmers/wordpress", true),
	},
	url:        "~charmers/trusty/wordpress-0",
	promulgate: true,
	expectEntities: []*mongodoc.Entity{
		entity("~charmers/trusty/wordpress-0", "trusty/wordpress-2"),
		entity("~test-charmers/trusty/wordpress-0", "trusty/wordpress-0"),
		entity("~test2-charmers/trusty/wordpress-0", "trusty/wordpress-1"),
	},
	expectBaseEntities: []*mongodoc.BaseEntity{
		baseEntity("~charmers/wordpress", true),
		baseEntity("~test-charmers/wordpress", false),
		baseEntity("~test2-charmers/wordpress", false),
	},
}, {
	about: "recovers from two promulgated base entities",
	entities: []*mongodoc.Entity{
		entity("~charmers/trusty/wordpress-0", ""),
		entity("~test-charmers/trusty/wordpress-0", "trusty/wordpress-0"),
		entity("~test-charmers/trusty/wordpress-1", "trusty/wordpress-2"),
		entity("~test2-charmers/trusty/wordpress-0", "trusty/wordpress-1"),
	},
	baseEntities: []*mongodoc.BaseEntity{
		baseEntity("~charmers/wordpress", false),
		baseEntity("~test-charmers/wordpress", true),
		baseEntity("~test2-charmers/wordpress", true),
	},
	url:        "~test2-charmers/trusty/wordpress-0",
	promulgate: true,
	expectEntities: []*mongodoc.Entity{
		entity("~charmers/trusty/wordpress-0", ""),
		entity("~test-charmers/trusty/wordpress-0", "trusty/wordpress-0"),
		entity("~test-charmers/trusty/wordpress-1", "trusty/wordpress-2"),
		entity("~test2-charmers/trusty/wordpress-0", "trusty/wordpress-1"),
	},
	expectBaseEntities: []*mongodoc.BaseEntity{
		baseEntity("~charmers/wordpress", false),
		baseEntity("~test-charmers/wordpress", false),
		baseEntity("~test2-charmers/wordpress", true),
	},
}, {
	about: "multiple series already promulgated",
	entities: []*mongodoc.Entity{
		entity("~charmers/trusty/wordpress-0", "trusty/wordpress-2"),
		entity("~charmers/precise/wordpress-0", "precise/wordpress-1"),
		entity("~test-charmers/trusty/wordpress-0", ""),
		entity("~test-charmers/utopic/wordpress-0", ""),
	},
	baseEntities: []*mongodoc.BaseEntity{
		baseEntity("~charmers/wordpress", true),
		baseEntity("~test-charmers/wordpress", false),
	},
	url:        "~test-charmers/trusty/wordpress-0",
	promulgate: true,
	expectEntities: []*mongodoc.Entity{
		entity("~charmers/trusty/wordpress-0", "trusty/wordpress-2"),
		entity("~charmers/precise/wordpress-0", "precise/wordpress-1"),
		entity("~test-charmers/trusty/wordpress-0", "trusty/wordpress-3"),
		entity("~test-charmers/utopic/wordpress-0", "utopic/wordpress-0"),
	},
	expectBaseEntities: []*mongodoc.BaseEntity{
		baseEntity("~charmers/wordpress", false),
		baseEntity("~test-charmers/wordpress", true),
	},
}, {
	about: "multi-series with old single series",
	entities: []*mongodoc.Entity{
		entity("~charmers/trusty/wordpress-0", "trusty/wordpress-2"),
		entity("~charmers/precise/wordpress-0", "precise/wordpress-3"),
		entity("~test-charmers/wordpress-0", "", "trusty", "precise", "xenial"),
	},
	baseEntities: []*mongodoc.BaseEntity{
		baseEntity("~charmers/wordpress", true),
		baseEntity("~test-charmers/wordpress", true),
	},
	url:        "~test-charmers/wordpress-0",
	promulgate: true,
	expectEntities: []*mongodoc.Entity{
		entity("~charmers/trusty/wordpress-0", "trusty/wordpress-2"),
		entity("~charmers/precise/wordpress-0", "precise/wordpress-3"),
		entity("~test-charmers/wordpress-0", "wordpress-4", "trusty", "precise", "xenial"),
	},
	expectBaseEntities: []*mongodoc.BaseEntity{
		baseEntity("~charmers/wordpress", false),
		baseEntity("~test-charmers/wordpress", true),
	},
}, {
	about: "multi-series with old multi-series",
	entities: []*mongodoc.Entity{
		entity("~charmers/trusty/wordpress-0", "trusty/wordpress-2"),
		entity("~charmers/precise/wordpress-0", "precise/wordpress-3"),
		entity("~charmers/wordpress-0", "wordpress-3", "trusty", "xenial"),
		entity("~test-charmers/trusty/wordpress-0", ""),
		entity("~test-charmers/precise/wordpress-3", ""),
		entity("~test-charmers/wordpress-4", "", "precise", "xenial"),
	},
	baseEntities: []*mongodoc.BaseEntity{
		baseEntity("~charmers/wordpress", true),
		baseEntity("~test-charmers/wordpress", true),
	},
	url:        "~test-charmers/wordpress-0",
	promulgate: true,
	expectEntities: []*mongodoc.Entity{
		entity("~charmers/trusty/wordpress-0", "trusty/wordpress-2"),
		entity("~charmers/precise/wordpress-0", "precise/wordpress-3"),
		entity("~charmers/wordpress-0", "wordpress-3", "trusty", "xenial"),
		entity("~test-charmers/trusty/wordpress-0", ""),
		entity("~test-charmers/precise/wordpress-3", ""),
		entity("~test-charmers/wordpress-4", "wordpress-4", "precise", "xenial"),
	},
	expectBaseEntities: []*mongodoc.BaseEntity{
		baseEntity("~charmers/wordpress", false),
		baseEntity("~test-charmers/wordpress", true),
	},
}, {
	about: "multi-series with different sets of supported series",
	entities: []*mongodoc.Entity{
		entity("~test-charmers/wordpress-0", "", "precise", "xenial"),
		entity("~test-charmers/wordpress-1", "", "trusty", "xenial"),
	},
	baseEntities: []*mongodoc.BaseEntity{
		baseEntity("~test-charmers/wordpress", false),
	},
	url:        "~test-charmers/wordpress-0",
	promulgate: true,
	expectEntities: []*mongodoc.Entity{
		entity("~test-charmers/wordpress-0", "", "precise", "xenial"),
		entity("~test-charmers/wordpress-1", "wordpress-0", "trusty", "xenial"),
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
		entity("~charmers/precise/wordpress-5", "wordpress-5"),
		entity("~charmers/wordpress-0", "wordpress-6", "precise", "xenial"),
		entity("~charmers/wordpress-1", "wordpress-7", "trusty", "xenial"),
		entity("~test-charmers/precise/wordpress-9", ""),
		entity("~test-charmers/wordpress-0", "", "trusty"),
		entity("~test-charmers/wordpress-1", "", "trusty", "xenial"),
	},
	baseEntities: []*mongodoc.BaseEntity{
		baseEntity("~charmers/wordpress", true),
		baseEntity("~test-charmers/wordpress", false),
	},
	url:        "~test-charmers/wordpress-1",
	promulgate: true,
	expectEntities: []*mongodoc.Entity{
		entity("~charmers/precise/wordpress-5", "wordpress-5"),
		entity("~charmers/wordpress-0", "wordpress-6", "precise", "xenial"),
		entity("~charmers/wordpress-1", "wordpress-7", "trusty", "xenial"),
		entity("~test-charmers/precise/wordpress-9", ""),
		entity("~test-charmers/wordpress-0", "", "trusty"),
		entity("~test-charmers/wordpress-1", "wordpress-8", "trusty", "xenial"),
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
		entity("~charmers/precise/wordpress-5", "wordpress-5"),
		entity("~charmers/wordpress-1", "wordpress-7", "trusty", "xenial"),
		entity("~test-charmers/precise/wordpress-9", ""),
	},
	baseEntities: []*mongodoc.BaseEntity{
		baseEntity("~charmers/wordpress", true),
		baseEntity("~test-charmers/wordpress", false),
	},
	url:        "~test-charmers/precise/wordpress-9",
	promulgate: true,
	expectEntities: []*mongodoc.Entity{
		entity("~charmers/precise/wordpress-5", "wordpress-5"),
		entity("~charmers/wordpress-1", "wordpress-7", "trusty", "xenial"),
		entity("~test-charmers/precise/wordpress-9", ""),
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
		entity("~charmers/trusty/wordpress-0", "trusty/wordpress-0"),
	},
	baseEntities: []*mongodoc.BaseEntity{
		baseEntity("~charmers/wordpress", true),
	},
	url:        "~charmers/trusty/wordpress-0",
	promulgate: false,
	expectEntities: []*mongodoc.Entity{
		entity("~charmers/trusty/wordpress-0", "trusty/wordpress-0"),
	},
	expectBaseEntities: []*mongodoc.BaseEntity{
		baseEntity("~charmers/wordpress", false),
	},
}, {
	about: "unpromulgate single unpromulgated charm ",
	entities: []*mongodoc.Entity{
		entity("~charmers/trusty/wordpress-0", ""),
	},
	baseEntities: []*mongodoc.BaseEntity{
		baseEntity("~charmers/wordpress", false),
	},
	url:        "~charmers/trusty/wordpress-0",
	promulgate: false,
	expectEntities: []*mongodoc.Entity{
		entity("~charmers/trusty/wordpress-0", ""),
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
		c.Assert(err, gc.IsNil)
		_, err = store.DB.BaseEntities().RemoveAll(nil)
		c.Assert(err, gc.IsNil)
		for _, entity := range test.entities {
			err := store.DB.Entities().Insert(entity)
			c.Assert(err, gc.IsNil)
		}
		for _, baseEntity := range test.baseEntities {
			err := store.DB.BaseEntities().Insert(baseEntity)
			c.Assert(err, gc.IsNil)
		}
		err = store.SetPromulgated(url, test.promulgate)
		if test.expectErr != "" {
			c.Assert(err, gc.ErrorMatches, test.expectErr)
			continue
		}
		c.Assert(err, gc.IsNil)
		n, err := store.DB.Entities().Count()
		c.Assert(err, gc.IsNil)
		c.Assert(n, gc.Equals, len(test.expectEntities))
		n, err = store.DB.BaseEntities().Count()
		c.Assert(err, gc.IsNil)
		c.Assert(n, gc.Equals, len(test.expectBaseEntities))
		for i, expectEntity := range test.expectEntities {
			entity, err := store.FindEntity(EntityResolvedURL(expectEntity), nil)
			c.Assert(err, gc.IsNil)
			c.Assert(entity, jc.DeepEquals, expectEntity, gc.Commentf("entity %d", i))
		}
		for _, expectBaseEntity := range test.expectBaseEntities {
			baseEntity, err := store.FindBaseEntity(expectBaseEntity.URL, nil)
			c.Assert(err, gc.IsNil)
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
		router.MustNewResolvedURL("~charmers/trusty/wordpress-0", 2),
		wordpress,
		nil,
		0,
	)
	addCharmForSearch(
		c,
		store,
		router.MustNewResolvedURL("~charmers/precise/wordpress-0", 1),
		wordpress,
		nil,
		0,
	)
	addCharmForSearch(
		c,
		store,
		router.MustNewResolvedURL("~openstack-charmers/trusty/wordpress-0", -1),
		wordpress,
		nil,
		0,
	)
	addCharmForSearch(
		c,
		store,
		router.MustNewResolvedURL("~openstack-charmers/precise/wordpress-0", -1),
		wordpress,
		nil,
		0,
	)
	url := router.MustNewResolvedURL("~openstack-charmers/trusty/wordpress-0", -1)

	// Change the promulgated wordpress version to openstack-charmers.
	err := store.SetPromulgated(url, true)
	c.Assert(err, gc.IsNil)
	err = store.ES.RefreshIndex(s.TestIndex)
	c.Assert(err, gc.IsNil)
	// Check that the search records contain the correct information.
	var zdoc SearchDoc
	doc := zdoc
	err = store.ES.GetDocument(s.TestIndex, typeName, store.ES.getID(charm.MustParseURL("~charmers/trusty/wordpress-0")), &doc)
	c.Assert(err, gc.IsNil)
	c.Assert(doc.PromulgatedURL, gc.IsNil)
	c.Assert(doc.PromulgatedRevision, gc.Equals, -1)
	doc = zdoc
	err = store.ES.GetDocument(s.TestIndex, typeName, store.ES.getID(charm.MustParseURL("~charmers/precise/wordpress-0")), &doc)
	c.Assert(err, gc.IsNil)
	c.Assert(doc.PromulgatedURL, gc.IsNil)
	c.Assert(doc.PromulgatedRevision, gc.Equals, -1)
	doc = zdoc
	err = store.ES.GetDocument(s.TestIndex, typeName, store.ES.getID(charm.MustParseURL("~openstack-charmers/trusty/wordpress-0")), &doc)
	c.Assert(err, gc.IsNil)
	c.Assert(doc.PromulgatedURL.String(), gc.Equals, "cs:trusty/wordpress-3")
	c.Assert(doc.PromulgatedRevision, gc.Equals, 3)
	doc = zdoc
	err = store.ES.GetDocument(s.TestIndex, typeName, store.ES.getID(charm.MustParseURL("~openstack-charmers/precise/wordpress-0")), &doc)
	c.Assert(err, gc.IsNil)
	c.Assert(doc.PromulgatedURL.String(), gc.Equals, "cs:precise/wordpress-2")
	c.Assert(doc.PromulgatedRevision, gc.Equals, 2)

	// Remove the promulgated flag from openstack-charmers, meaning wordpress is
	// no longer promulgated.
	err = store.SetPromulgated(url, false)
	c.Assert(err, gc.IsNil)
	err = store.ES.RefreshIndex(s.TestIndex)
	c.Assert(err, gc.IsNil)
	// Check that the search records contain the correct information.
	doc = zdoc
	err = store.ES.GetDocument(s.TestIndex, typeName, store.ES.getID(charm.MustParseURL("~charmers/trusty/wordpress-0")), &doc)
	c.Assert(err, gc.IsNil)
	c.Assert(doc.PromulgatedURL, gc.IsNil)
	c.Assert(doc.PromulgatedRevision, gc.Equals, -1)
	doc = zdoc
	err = store.ES.GetDocument(s.TestIndex, typeName, store.ES.getID(charm.MustParseURL("~charmers/precise/wordpress-0")), &doc)
	c.Assert(err, gc.IsNil)
	c.Assert(doc.PromulgatedURL, gc.IsNil)
	c.Assert(doc.PromulgatedRevision, gc.Equals, -1)
	doc = zdoc
	err = store.ES.GetDocument(s.TestIndex, typeName, store.ES.getID(charm.MustParseURL("~openstack-charmers/trusty/wordpress-0")), &doc)
	c.Assert(err, gc.IsNil)
	c.Assert(doc.PromulgatedURL, gc.IsNil)
	c.Assert(doc.PromulgatedRevision, gc.Equals, -1)
	doc = zdoc
	err = store.ES.GetDocument(s.TestIndex, typeName, store.ES.getID(charm.MustParseURL("~openstack-charmers/precise/wordpress-0")), &doc)
	c.Assert(err, gc.IsNil)
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
		URL: charm.MustParseURL("~charmers/precise/wordpress-23"),
	},
	rurl: &router.ResolvedURL{
		URL:                 *charm.MustParseURL("~charmers/precise/wordpress-23"),
		PromulgatedRevision: -1,
	},
}, {
	about: "promulgated, published",
	entity: &mongodoc.Entity{
		URL:            charm.MustParseURL("~charmers/precise/wordpress-23"),
		PromulgatedURL: charm.MustParseURL("precise/wordpress-4"),
	},
	rurl: &router.ResolvedURL{
		URL:                 *charm.MustParseURL("~charmers/precise/wordpress-23"),
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

	wordpress := storetesting.Charms.CharmDir("wordpress")
	url := MustParseResolvedURL("23 cs:~charmers/precise/wordpress-23")
	err := store.AddCharmWithArchive(url, wordpress)
	c.Assert(err, gc.IsNil)

	store1 := store.Copy()
	defer store1.Close()

	// Close the store we copied from. The copy should be unaffected.
	store.Close()

	entity, err := store1.FindEntity(url, nil)
	c.Assert(err, gc.IsNil)

	// Also check the blob store, as it has its own session reference.
	r, _, err := store1.BlobStore.Open(entity.BlobName)
	c.Assert(err, gc.IsNil)
	r.Close()

	// Also check the macaroon storage as that also has its own session reference.
	m, err := store1.Bakery.NewMacaroon("", nil, nil)
	c.Assert(err, gc.IsNil)
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
	c.Assert(err, gc.IsNil)
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
	c.Assert(err, gc.IsNil)

	lines := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
	c.Assert(lines, gc.HasLen, len(entries))
	for i, e := range entries {
		e.Time = now
		c.Assert(lines[i], jc.JSONEquals, e)
	}
}

func (s *StoreSuite) TestAddAuditWithNoLumberjack(c *gc.C) {
	p, err := NewPool(s.Session.DB("juju_test"), nil, nil, ServerParams{})
	c.Assert(err, gc.IsNil)
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
	c.Assert(err, gc.IsNil)
	err = store.Publish(rurl, nil, params.StableChannel)
	c.Assert(err, gc.IsNil)
	riak := storetesting.Charms.CharmArchive(c.MkDir(), "riak")
	rurl = router.MustNewResolvedURL("cs:~charmers/trusty/riak-42", 42)
	err = store.AddCharmWithArchive(rurl, riak)
	c.Assert(err, gc.IsNil)
	err = store.Publish(rurl, nil, params.StableChannel)
	c.Assert(err, gc.IsNil)
	wordpress := storetesting.Charms.CharmArchive(c.MkDir(), "wordpress")
	rurl = router.MustNewResolvedURL("cs:~charmers/utopic/wordpress-47", 47)
	err = store.AddCharmWithArchive(rurl, wordpress)
	c.Assert(err, gc.IsNil)
	err = store.Publish(rurl, nil, params.StableChannel)
	c.Assert(err, gc.IsNil)

	tests := []struct {
		about  string
		ids    []string
		charms map[string]charm.Charm
	}{{
		about: "no ids",
	}, {
		about: "fully qualified ids",
		ids: []string{
			"cs:~charmers/saucy/mysql-0",
			"cs:~charmers/trusty/riak-42",
			"cs:~charmers/utopic/wordpress-47",
		},
		charms: map[string]charm.Charm{
			"cs:~charmers/saucy/mysql-0":       mysql,
			"cs:~charmers/trusty/riak-42":      riak,
			"cs:~charmers/utopic/wordpress-47": wordpress,
		},
	}, {
		about: "partial ids",
		ids:   []string{"~charmers/utopic/wordpress", "~charmers/riak"},
		charms: map[string]charm.Charm{
			"~charmers/riak":             riak,
			"~charmers/utopic/wordpress": wordpress,
		},
	}, {
		about: "charm not found",
		ids:   []string{"utopic/no-such", "~charmers/mysql"},
		charms: map[string]charm.Charm{
			"~charmers/mysql": mysql,
		},
	}, {
		about: "no charms found",
		ids: []string{
			"cs:~charmers/saucy/mysql-99",   // Revision not present.
			"cs:~charmers/precise/riak-42",  // Series not present.
			"cs:~charmers/utopic/django-47", // Name not present.
		},
	}, {
		about: "repeated charms",
		ids: []string{
			"cs:~charmers/saucy/mysql",
			"cs:~charmers/trusty/riak-42",
			"~charmers/mysql",
		},
		charms: map[string]charm.Charm{
			"cs:~charmers/saucy/mysql":    mysql,
			"cs:~charmers/trusty/riak-42": riak,
			"~charmers/mysql":             mysql,
		},
	}}

	// Run the tests.
	for i, test := range tests {
		c.Logf("test %d: %s", i, test.about)
		charms, err := store.bundleCharms(test.ids)
		c.Assert(err, gc.IsNil)
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
	about:    "unpublished, single series, publish development",
	url:      MustParseResolvedURL("~who/trusty/django-42"),
	channels: []params.Channel{params.DevelopmentChannel},
	initialEntity: &mongodoc.Entity{
		URL: charm.MustParseURL("~who/trusty/django-42"),
	},
	initialBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
	},
	expectedEntity: &mongodoc.Entity{
		URL:         charm.MustParseURL("~who/trusty/django-42"),
		Development: true,
	},
	expectedBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		ChannelEntities: map[params.Channel]map[string]*charm.URL{
			params.DevelopmentChannel: {
				"trusty": charm.MustParseURL("~who/trusty/django-42"),
			},
		},
	},
}, {
	about:    "development, single series, publish development",
	url:      MustParseResolvedURL("~who/trusty/django-42"),
	channels: []params.Channel{params.DevelopmentChannel},
	initialEntity: &mongodoc.Entity{
		URL:         charm.MustParseURL("~who/trusty/django-42"),
		Development: true,
	},
	initialBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		ChannelEntities: map[params.Channel]map[string]*charm.URL{
			params.DevelopmentChannel: {
				"trusty": charm.MustParseURL("~who/trusty/django-41"),
			},
		},
	},
	expectedEntity: &mongodoc.Entity{
		URL:         charm.MustParseURL("~who/trusty/django-42"),
		Development: true,
	},
	expectedBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		ChannelEntities: map[params.Channel]map[string]*charm.URL{
			params.DevelopmentChannel: {
				"trusty": charm.MustParseURL("~who/trusty/django-42"),
			},
		},
	},
}, {
	about:    "stable, single series, publish development",
	url:      MustParseResolvedURL("~who/trusty/django-42"),
	channels: []params.Channel{params.DevelopmentChannel},
	initialEntity: &mongodoc.Entity{
		URL:    charm.MustParseURL("~who/trusty/django-42"),
		Stable: true,
	},
	initialBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		ChannelEntities: map[params.Channel]map[string]*charm.URL{
			params.StableChannel: {
				"trusty": charm.MustParseURL("~who/trusty/django-42"),
			},
		},
	},
	expectedEntity: &mongodoc.Entity{
		URL:         charm.MustParseURL("~who/trusty/django-42"),
		Stable:      true,
		Development: true,
	},
	expectedBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		ChannelEntities: map[params.Channel]map[string]*charm.URL{
			params.StableChannel: {
				"trusty": charm.MustParseURL("~who/trusty/django-42"),
			},
			params.DevelopmentChannel: {
				"trusty": charm.MustParseURL("~who/trusty/django-42"),
			},
		},
	},
}, {
	about:    "unpublished, single series, publish stable",
	url:      MustParseResolvedURL("~who/trusty/django-42"),
	channels: []params.Channel{params.StableChannel},
	initialEntity: &mongodoc.Entity{
		URL: charm.MustParseURL("~who/trusty/django-42"),
	},
	initialBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
	},
	expectedEntity: &mongodoc.Entity{
		URL:    charm.MustParseURL("~who/trusty/django-42"),
		Stable: true,
	},
	expectedBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		ChannelEntities: map[params.Channel]map[string]*charm.URL{
			params.StableChannel: {
				"trusty": charm.MustParseURL("~who/trusty/django-42"),
			},
		},
	},
}, {
	about:    "development, single series, publish stable",
	url:      MustParseResolvedURL("~who/trusty/django-42"),
	channels: []params.Channel{params.StableChannel},
	initialEntity: &mongodoc.Entity{
		URL:         charm.MustParseURL("~who/trusty/django-42"),
		Development: true,
	},
	initialBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		ChannelEntities: map[params.Channel]map[string]*charm.URL{
			params.DevelopmentChannel: {
				"trusty": charm.MustParseURL("~who/trusty/django-41"),
			},
		},
	},
	expectedEntity: &mongodoc.Entity{
		URL:         charm.MustParseURL("~who/trusty/django-42"),
		Development: true,
		Stable:      true,
	},
	expectedBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		ChannelEntities: map[params.Channel]map[string]*charm.URL{
			params.DevelopmentChannel: {
				"trusty": charm.MustParseURL("~who/trusty/django-41"),
			},
			params.StableChannel: {
				"trusty": charm.MustParseURL("~who/trusty/django-42"),
			},
		},
	},
}, {
	about:    "stable, single series, publish stable",
	url:      MustParseResolvedURL("~who/trusty/django-42"),
	channels: []params.Channel{params.StableChannel},
	initialEntity: &mongodoc.Entity{
		URL:    charm.MustParseURL("~who/trusty/django-42"),
		Stable: true,
	},
	initialBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		ChannelEntities: map[params.Channel]map[string]*charm.URL{
			params.StableChannel: {
				"trusty": charm.MustParseURL("~who/trusty/django-40"),
			},
		},
	},
	expectedEntity: &mongodoc.Entity{
		URL:    charm.MustParseURL("~who/trusty/django-42"),
		Stable: true,
	},
	expectedBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		ChannelEntities: map[params.Channel]map[string]*charm.URL{
			params.StableChannel: {
				"trusty": charm.MustParseURL("~who/trusty/django-42"),
			},
		},
	},
}, {
	about:    "unpublished, multi series, publish development",
	url:      MustParseResolvedURL("~who/django-42"),
	channels: []params.Channel{params.DevelopmentChannel},
	initialEntity: &mongodoc.Entity{
		URL:             charm.MustParseURL("~who/django-42"),
		SupportedSeries: []string{"trusty", "wily"},
	},
	initialBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
	},
	expectedEntity: &mongodoc.Entity{
		URL:             charm.MustParseURL("~who/django-42"),
		SupportedSeries: []string{"trusty", "wily"},
		Development:     true,
	},
	expectedBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		ChannelEntities: map[params.Channel]map[string]*charm.URL{
			params.DevelopmentChannel: {
				"trusty": charm.MustParseURL("~who/django-42"),
				"wily":   charm.MustParseURL("~who/django-42"),
			},
		},
	},
}, {
	about:    "development, multi series, publish development",
	url:      MustParseResolvedURL("~who/django-42"),
	channels: []params.Channel{params.DevelopmentChannel},
	initialEntity: &mongodoc.Entity{
		URL:             charm.MustParseURL("~who/django-42"),
		Development:     true,
		SupportedSeries: []string{"trusty", "wily"},
	},
	initialBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		ChannelEntities: map[params.Channel]map[string]*charm.URL{
			params.DevelopmentChannel: {
				"precise": charm.MustParseURL("~who/django-0"),
				"trusty":  charm.MustParseURL("~who/trusty/django-0"),
			},
		},
	},
	expectedEntity: &mongodoc.Entity{
		URL:             charm.MustParseURL("~who/django-42"),
		Development:     true,
		SupportedSeries: []string{"trusty", "wily"},
	},
	expectedBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		ChannelEntities: map[params.Channel]map[string]*charm.URL{
			params.DevelopmentChannel: {
				"precise": charm.MustParseURL("~who/django-0"),
				"trusty":  charm.MustParseURL("~who/django-42"),
				"wily":    charm.MustParseURL("~who/django-42"),
			},
		},
	},
}, {
	about:    "stable, multi series, publish development",
	url:      MustParseResolvedURL("~who/django-47"),
	channels: []params.Channel{params.DevelopmentChannel},
	initialEntity: &mongodoc.Entity{
		URL:             charm.MustParseURL("~who/django-47"),
		SupportedSeries: []string{"trusty", "wily", "precise"},
		Stable:          true,
	},
	initialBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		ChannelEntities: map[params.Channel]map[string]*charm.URL{
			params.StableChannel: {
				"trusty": charm.MustParseURL("~who/django-47"),
			},
		},
	},
	expectedEntity: &mongodoc.Entity{
		URL:             charm.MustParseURL("~who/django-47"),
		SupportedSeries: []string{"trusty", "wily", "precise"},
		Stable:          true,
		Development:     true,
	},
	expectedBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		ChannelEntities: map[params.Channel]map[string]*charm.URL{
			params.StableChannel: {
				"trusty": charm.MustParseURL("~who/django-47"),
			},
			params.DevelopmentChannel: {
				"trusty":  charm.MustParseURL("~who/django-47"),
				"wily":    charm.MustParseURL("~who/django-47"),
				"precise": charm.MustParseURL("~who/django-47"),
			},
		},
	},
}, {
	about:    "unpublished, multi series, publish stable",
	url:      MustParseResolvedURL("~who/django-42"),
	channels: []params.Channel{params.StableChannel},
	initialEntity: &mongodoc.Entity{
		URL:             charm.MustParseURL("~who/django-42"),
		SupportedSeries: []string{"trusty", "wily", "precise"},
	},
	initialBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
	},
	expectedEntity: &mongodoc.Entity{
		URL:             charm.MustParseURL("~who/django-42"),
		SupportedSeries: []string{"trusty", "wily", "precise"},
		Stable:          true,
	},
	expectedBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		ChannelEntities: map[params.Channel]map[string]*charm.URL{
			params.StableChannel: {
				"trusty":  charm.MustParseURL("~who/django-42"),
				"wily":    charm.MustParseURL("~who/django-42"),
				"precise": charm.MustParseURL("~who/django-42"),
			},
		},
	},
}, {
	about:    "development, multi series, publish stable",
	url:      MustParseResolvedURL("~who/django-42"),
	channels: []params.Channel{params.StableChannel},
	initialEntity: &mongodoc.Entity{
		URL:             charm.MustParseURL("~who/django-42"),
		SupportedSeries: []string{"wily"},
		Development:     true,
	},
	initialBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		ChannelEntities: map[params.Channel]map[string]*charm.URL{
			params.DevelopmentChannel: {
				"trusty": charm.MustParseURL("~who/django-0"),
			},
		},
	},
	expectedEntity: &mongodoc.Entity{
		URL:             charm.MustParseURL("~who/django-42"),
		SupportedSeries: []string{"wily"},
		Development:     true,
		Stable:          true,
	},
	expectedBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		ChannelEntities: map[params.Channel]map[string]*charm.URL{
			params.StableChannel: {
				"wily": charm.MustParseURL("~who/django-42"),
			},
			params.DevelopmentChannel: {
				"trusty": charm.MustParseURL("~who/django-0"),
			},
		},
	},
}, {
	about:    "stable, multi series, publish stable",
	url:      MustParseResolvedURL("~who/django-42"),
	channels: []params.Channel{params.StableChannel},
	initialEntity: &mongodoc.Entity{
		URL:             charm.MustParseURL("~who/django-42"),
		SupportedSeries: []string{"trusty", "wily", "precise"},
		Stable:          true,
	},
	initialBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		ChannelEntities: map[params.Channel]map[string]*charm.URL{
			params.StableChannel: {
				"precise": charm.MustParseURL("~who/django-1"),
				"quantal": charm.MustParseURL("~who/django-2"),
				"saucy":   charm.MustParseURL("~who/django-3"),
				"trusty":  charm.MustParseURL("~who/django-4"),
			},
		},
	},
	expectedEntity: &mongodoc.Entity{
		URL:             charm.MustParseURL("~who/django-42"),
		SupportedSeries: []string{"trusty", "wily", "precise"},
		Stable:          true,
	},
	expectedBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		ChannelEntities: map[params.Channel]map[string]*charm.URL{
			params.StableChannel: {
				"precise": charm.MustParseURL("~who/django-42"),
				"quantal": charm.MustParseURL("~who/django-2"),
				"saucy":   charm.MustParseURL("~who/django-3"),
				"trusty":  charm.MustParseURL("~who/django-42"),
				"wily":    charm.MustParseURL("~who/django-42"),
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
		URL:    charm.MustParseURL("~who/bundle/django-42"),
		Stable: true,
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
	about:    "unpublished, multi series, publish multiple channels",
	url:      MustParseResolvedURL("~who/django-42"),
	channels: []params.Channel{params.DevelopmentChannel, params.StableChannel, params.Channel("no-such")},
	initialEntity: &mongodoc.Entity{
		URL:             charm.MustParseURL("~who/django-42"),
		SupportedSeries: []string{"trusty", "wily"},
	},
	initialBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		ChannelEntities: map[params.Channel]map[string]*charm.URL{
			params.StableChannel: {
				"quantal": charm.MustParseURL("~who/django-1"),
				"trusty":  charm.MustParseURL("~who/django-4"),
			},
			params.DevelopmentChannel: {
				"wily": charm.MustParseURL("~who/django-10"),
			},
		},
	},
	expectedEntity: &mongodoc.Entity{
		URL:             charm.MustParseURL("~who/django-42"),
		SupportedSeries: []string{"trusty", "wily"},
		Development:     true,
		Stable:          true,
	},
	expectedBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		ChannelEntities: map[params.Channel]map[string]*charm.URL{
			params.DevelopmentChannel: {
				"trusty": charm.MustParseURL("~who/django-42"),
				"wily":   charm.MustParseURL("~who/django-42"),
			},
			params.StableChannel: {
				"quantal": charm.MustParseURL("~who/django-1"),
				"trusty":  charm.MustParseURL("~who/django-42"),
				"wily":    charm.MustParseURL("~who/django-42"),
			},
		},
	},
}, {
	about:    "not found",
	url:      MustParseResolvedURL("~who/trusty/no-such-42"),
	channels: []params.Channel{params.DevelopmentChannel},
	initialEntity: &mongodoc.Entity{
		URL: charm.MustParseURL("~who/trusty/django-42"),
	},
	initialBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
	},
	expectedErr: `entity not found`,
}, {
	about:    "no valid channels provided",
	url:      MustParseResolvedURL("~who/trusty/django-42"),
	channels: []params.Channel{params.Channel("not-valid")},
	initialEntity: &mongodoc.Entity{
		URL: charm.MustParseURL("~who/trusty/django-42"),
	},
	initialBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
	},
	expectedErr: `cannot update "cs:~who/trusty/django-42": no valid channels provided`,
}}

func (s *StoreSuite) TestPublish(c *gc.C) {
	store := s.newStore(c, true)
	defer store.Close()

	for i, test := range publishTests {
		c.Logf("test %d: %s", i, test.about)

		// Remove existing entities and base entities.
		_, err := store.DB.Entities().RemoveAll(nil)
		c.Assert(err, gc.IsNil)
		_, err = store.DB.BaseEntities().RemoveAll(nil)
		c.Assert(err, gc.IsNil)
		// Insert the existing entity.
		err = store.DB.Entities().Insert(denormalizedEntity(test.initialEntity))
		c.Assert(err, gc.IsNil)
		// Insert the existing base entity.
		err = store.DB.BaseEntities().Insert(test.initialBaseEntity)
		c.Assert(err, gc.IsNil)

		// Publish the entity.
		err = store.Publish(test.url, nil, test.channels...)
		if test.expectedErr != "" {
			c.Assert(err, gc.ErrorMatches, test.expectedErr)
			continue
		}
		c.Assert(err, gc.IsNil)
		entity, err := store.FindEntity(test.url, nil)
		c.Assert(err, gc.IsNil)
		c.Assert(entity, jc.DeepEquals, denormalizedEntity(test.expectedEntity))
		baseEntity, err := store.FindBaseEntity(&test.url.URL, nil)
		c.Assert(err, gc.IsNil)
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

	url := router.MustNewResolvedURL("~charmers/precise/wordpress-12", -1)
	err := store.AddCharmWithArchive(url, storetesting.Charms.CharmDir("wordpress"))
	c.Assert(err, gc.IsNil)
	err = store.Publish(url, nil, params.StableChannel)
	c.Assert(err, gc.ErrorMatches, "cannot index cs:~charmers/precise/wordpress-12 to ElasticSearch: .*")
}

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
