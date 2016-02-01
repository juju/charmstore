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
	"gopkg.in/macaroon-bakery.v1/bakery"
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
	storetesting.IsolatedMgoESSuite
	index string
}

var _ = gc.Suite(&StoreSuite{})

func (s *StoreSuite) checkAddCharm(c *gc.C, ch charm.Charm, addToES bool, url *router.ResolvedURL) {
	var es *elasticsearch.Database
	if addToES {
		es = s.ES
	}
	store := s.newStore(c, true)
	defer store.Close()

	// Add the charm to the store.
	beforeAdding := time.Now()
	err := store.AddCharmWithArchive(url, ch)
	c.Assert(err, gc.IsNil)
	afterAdding := time.Now()

	// Publish the charm as stable.
	err = store.Publish(url, StableChannel)
	c.Assert(err, gc.IsNil)

	var doc *mongodoc.Entity
	err = store.DB.Entities().FindId(&url.URL).One(&doc)
	c.Assert(err, gc.IsNil)

	// Ensure the document was indexed in ElasticSearch, if an ES database was provided.
	if es != nil {
		var result SearchDoc
		id := store.ES.getID(doc.URL)
		err = store.ES.GetDocument(s.TestIndex, typeName, id, &result)
		c.Assert(err, gc.IsNil)
		exists, err := store.ES.HasDocument(s.TestIndex, typeName, id)
		c.Assert(err, gc.IsNil)
		c.Assert(exists, gc.Equals, true)
		if purl := url.DocPromulgatedURL(); purl != nil {
			c.Assert(result.PromulgatedURL, jc.DeepEquals, purl)
		}
	}
	// The entity doc has been correctly added to the mongo collection.
	size, hash, hash256 := getSizeAndHashes(ch)
	sort.Strings(doc.CharmProvidedInterfaces)
	sort.Strings(doc.CharmRequiredInterfaces)

	// Check the upload time and then reset it to its zero value
	// so that we can test the deterministic parts later.
	c.Assert(doc.UploadTime, jc.TimeBetween(beforeAdding, afterAdding))

	doc.UploadTime = time.Time{}

	blobName := doc.BlobName
	c.Assert(blobName, gc.Matches, "[0-9a-z]+")
	doc.BlobName = ""

	c.Assert(doc, jc.DeepEquals, denormalizedEntity(&mongodoc.Entity{
		URL:                     &url.URL,
		BlobHash:                hash,
		BlobHash256:             hash256,
		Size:                    size,
		CharmMeta:               ch.Meta(),
		CharmActions:            ch.Actions(),
		CharmConfig:             ch.Config(),
		CharmProvidedInterfaces: []string{"http", "logging", "monitoring"},
		CharmRequiredInterfaces: []string{"mysql", "varnish"},
		PromulgatedURL:          url.DocPromulgatedURL(),
		SupportedSeries:         ch.Meta().Series,
		Development:             url.Development,
		Stable:                  true,
	}))

	// The charm archive has been properly added to the blob store.
	r, obtainedSize, err := store.BlobStore.Open(blobName)
	c.Assert(err, gc.IsNil)
	defer r.Close()
	c.Assert(obtainedSize, gc.Equals, size)
	data, err := ioutil.ReadAll(r)
	c.Assert(err, gc.IsNil)
	charmArchive, err := charm.ReadCharmArchiveBytes(data)
	c.Assert(err, gc.IsNil)
	c.Assert(charmArchive.Meta(), jc.DeepEquals, ch.Meta())
	c.Assert(charmArchive.Config(), jc.DeepEquals, ch.Config())
	c.Assert(charmArchive.Actions(), jc.DeepEquals, ch.Actions())
	c.Assert(charmArchive.Revision(), jc.DeepEquals, ch.Revision())

	// Check that the base entity has been properly created.
	assertBaseEntity(c, store, doc, url.PromulgatedRevision != -1)

	// Try inserting the charm again - it should fail because the charm is
	// already there.
	err = store.AddCharmWithArchive(url, ch)
	c.Assert(errgo.Cause(err), gc.Equals, params.ErrDuplicateUpload)
}

func (s *StoreSuite) checkAddBundle(c *gc.C, bundle charm.Bundle, addToES bool, url *router.ResolvedURL) {
	var es *elasticsearch.Database

	if addToES {
		es = s.ES
	}
	store := s.newStore(c, true)
	defer store.Close()

	// Add the bundle to the store.
	beforeAdding := time.Now()
	err := store.AddBundleWithArchive(url, bundle)
	c.Assert(err, gc.IsNil)
	afterAdding := time.Now()

	// Publish the bundle as stable.
	err = store.Publish(url, StableChannel)
	c.Assert(err, gc.IsNil)

	var doc *mongodoc.Entity
	err = store.DB.Entities().FindId(&url.URL).One(&doc)
	c.Assert(err, gc.IsNil)
	sort.Sort(orderedURLs(doc.BundleCharms))

	// Ensure the document was indexed in ElasticSearch, if an ES database was provided.
	if es != nil {
		var result SearchDoc
		id := store.ES.getID(doc.URL)
		err = store.ES.GetDocument(s.TestIndex, typeName, id, &result)
		c.Assert(err, gc.IsNil)
		exists, err := store.ES.HasDocument(s.TestIndex, typeName, id)
		c.Assert(err, gc.IsNil)
		c.Assert(exists, gc.Equals, true)
		if purl := url.PromulgatedURL(); purl != nil {
			c.Assert(result.PromulgatedURL, jc.DeepEquals, purl)
		}
	}

	// Check the upload time and then reset it to its zero value
	// so that we can test the deterministic parts later.
	c.Assert(doc.UploadTime, jc.TimeBetween(beforeAdding, afterAdding))
	doc.UploadTime = time.Time{}

	// The blob name is random, but we check that it's
	// in the correct format, and non-empty.
	blobName := doc.BlobName
	c.Assert(blobName, gc.Matches, "[0-9a-z]+")
	doc.BlobName = ""

	// The entity doc has been correctly added to the mongo collection.
	size, hash, hash256 := getSizeAndHashes(bundle)
	c.Assert(doc, jc.DeepEquals, denormalizedEntity(&mongodoc.Entity{
		URL:          &url.URL,
		BlobHash:     hash,
		BlobHash256:  hash256,
		Size:         size,
		BundleData:   bundle.Data(),
		BundleReadMe: bundle.ReadMe(),
		BundleCharms: []*charm.URL{
			charm.MustParseURL("mysql"),
			charm.MustParseURL("wordpress"),
		},
		BundleMachineCount: newInt(2),
		BundleUnitCount:    newInt(2),
		PromulgatedURL:     url.DocPromulgatedURL(),
		Development:        url.Development,
		Stable:             true,
	}))

	// The bundle archive has been properly added to the blob store.
	r, obtainedSize, err := store.BlobStore.Open(blobName)
	c.Assert(err, gc.IsNil)
	defer r.Close()
	c.Assert(obtainedSize, gc.Equals, size)
	data, err := ioutil.ReadAll(r)
	c.Assert(err, gc.IsNil)
	bundleArchive, err := charm.ReadBundleArchiveBytes(data)
	c.Assert(err, gc.IsNil)
	c.Assert(bundleArchive.Data(), jc.DeepEquals, bundle.Data())
	c.Assert(bundleArchive.ReadMe(), jc.DeepEquals, bundle.ReadMe())

	// Check that the base entity has been properly created.
	assertBaseEntity(c, store, doc, url.PromulgatedRevision != -1)

	// Try inserting the bundle again - it should fail because the bundle is
	// already there.
	err = store.AddBundleWithArchive(url, bundle)
	c.Assert(errgo.Cause(err), gc.Equals, params.ErrDuplicateUpload)
}

func assertBaseEntity(c *gc.C, store *Store, entity *mongodoc.Entity, promulgated bool) {
	url := mongodoc.BaseURL(entity.URL)
	baseEntity, err := store.FindBaseEntity(url, nil)
	c.Assert(err, gc.IsNil)
	expectACLs := mongodoc.ACL{
		Read:  []string{url.User},
		Write: []string{url.User},
	}
	var expectStableSeries map[string]*charm.URL
	if entity.Stable {
		series := entity.SupportedSeries
		if len(series) == 0 {
			series = []string{entity.URL.Series}
		}
		expectStableSeries = make(map[string]*charm.URL, len(series))
		for _, s := range series {
			expectStableSeries[s] = entity.URL
		}
	}
	c.Assert(baseEntity, jc.DeepEquals, &mongodoc.BaseEntity{
		URL:             url,
		User:            url.User,
		Name:            url.Name,
		Public:          false,
		ACLs:            expectACLs,
		DevelopmentACLs: expectACLs,
		StableACLs:      expectACLs,
		Promulgated:     mongodoc.IntBool(promulgated),
		StableSeries:    expectStableSeries,
	})
}

type orderedURLs []*charm.URL

func (o orderedURLs) Less(i, j int) bool {
	return o[i].String() < o[j].String()
}

func (o orderedURLs) Swap(i, j int) {
	o[i], o[j] = o[j], o[i]
}

func (o orderedURLs) Len() int {
	return len(o)
}

var urlFindingTests = []struct {
	inStore []string
	expand  string
	expect  []string
}{{
	inStore: []string{"23 cs:~charmers/precise/wordpress-23"},
	expand:  "wordpress",
	expect:  []string{"23 cs:~charmers/precise/wordpress-23"},
}, {
	inStore: []string{"23 cs:~charmers/development/precise/wordpress-23"},
	expand:  "wordpress",
	expect:  []string{"23 cs:~charmers/development/precise/wordpress-23"},
}, {
	inStore: []string{"23 cs:~charmers/development/precise/wordpress-23"},
	expand:  "development/wordpress",
	expect:  []string{"23 cs:~charmers/development/precise/wordpress-23"},
}, {
	inStore: []string{"23 cs:~charmers/precise/wordpress-23"},
	expand:  "development/wordpress",
	expect:  []string{},
}, {
	inStore: []string{"23 cs:~charmers/precise/wordpress-23", "24 cs:~charmers/precise/wordpress-24", "25 cs:~charmers/development/precise/wordpress-25"},
	expand:  "wordpress",
	expect:  []string{"23 cs:~charmers/precise/wordpress-23", "24 cs:~charmers/precise/wordpress-24", "25 cs:~charmers/development/precise/wordpress-25"},
}, {
	inStore: []string{"23 cs:~charmers/precise/wordpress-23", "24 cs:~charmers/precise/wordpress-24", "25 cs:~charmers/development/precise/wordpress-26"},
	expand:  "~charmers/precise/wordpress-24",
	expect:  []string{"24 cs:~charmers/precise/wordpress-24"},
}, {
	inStore: []string{"23 cs:~charmers/precise/wordpress-23", "24 cs:~charmers/precise/wordpress-24", "25 cs:~charmers/development/precise/wordpress-25"},
	expand:  "~charmers/development/precise/wordpress-25",
	expect:  []string{"25 cs:~charmers/development/precise/wordpress-25"},
}, {
	inStore: []string{"23 cs:~charmers/precise/wordpress-23", "24 cs:~charmers/precise/wordpress-24", "25 cs:~charmers/development/precise/wordpress-25"},
	expand:  "~charmers/precise/wordpress-25",
	expect:  []string{"25 cs:~charmers/development/precise/wordpress-25"},
}, {
	inStore: []string{"23 cs:~charmers/precise/wordpress-23", "24 cs:~charmers/precise/wordpress-24", "25 cs:~charmers/development/precise/wordpress-25"},
	expand:  "development/wordpress",
	expect:  []string{"25 cs:~charmers/development/precise/wordpress-25"},
}, {
	inStore: []string{"23 cs:~charmers/precise/wordpress-23", "24 cs:~charmers/trusty/wordpress-24", "25 cs:~charmers/development/wily/wordpress-25"},
	expand:  "precise/wordpress",
	expect:  []string{"23 cs:~charmers/precise/wordpress-23"},
}, {
	inStore: []string{"23 cs:~charmers/precise/wordpress-23", "24 cs:~charmers/trusty/wordpress-24", "25 cs:~charmers/development/precise/wordpress-25", "27 cs:~charmers/development/precise/wordpress-27", "26 cs:~charmers/development/wily/wordpress-26"},
	expand:  "development/precise/wordpress",
	expect:  []string{"25 cs:~charmers/development/precise/wordpress-25", "27 cs:~charmers/development/precise/wordpress-27"},
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
}, {
	inStore: []string{},
	expand:  "development/precise/wordpress-23",
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
			if url.Development {
				err = store.Publish(url, charm.DevelopmentChannel)
				c.Assert(err, gc.IsNil)
			}
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
				PromulgatedURL: url.DocPromulgatedURL(),
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
	s.testURLFinding(c, func(store *Store, expand *charm.URL, expect []*router.ResolvedURL) {
		if expand.Series == "" || expand.Revision == -1 || expand.User == "" {
			return
		}
		rurl := &router.ResolvedURL{
			URL:                 *expand.WithChannel(""),
			PromulgatedRevision: -1,
			Development:         expand.Channel == charm.DevelopmentChannel,
		}
		entity, err := store.FindEntity(rurl, FieldSelector("_id", "promulgated-url", "development"))
		if len(expect) == 0 {
			c.Assert(err, gc.ErrorMatches, "entity not found")
			c.Assert(errgo.Cause(err), gc.Equals, params.ErrNotFound)
			return
		}
		c.Assert(err, gc.IsNil)
		c.Assert(len(expect), gc.Equals, 1)
		c.Assert(entity.BlobName, gc.Equals, "")
		c.Assert(EntityResolvedURL(entity), jc.DeepEquals, expect[0])

		// Check that it works when returning other fields too.
		entity, err = store.FindEntity(rurl, FieldSelector("blobname"))
		c.Assert(err, gc.IsNil)
		c.Assert(entity.BlobName, gc.Not(gc.Equals), "")
	})
}

var findBaseEntityTests = []struct {
	about  string
	stored []string
	url    string
	fields []string
	expect *mongodoc.BaseEntity
}{{
	about:  "entity found, base url, all fields",
	stored: []string{"42 cs:~charmers/utopic/django-42"},
	url:    "django",
	expect: &mongodoc.BaseEntity{
		URL:         charm.MustParseURL("~charmers/django"),
		User:        "charmers",
		Name:        "django",
		Public:      false,
		Promulgated: true,
		ACLs: mongodoc.ACL{
			Read:  []string{"charmers"},
			Write: []string{"charmers"},
		},
		DevelopmentACLs: mongodoc.ACL{
			Read:  []string{"charmers"},
			Write: []string{"charmers"},
		},
		StableACLs: mongodoc.ACL{
			Read:  []string{"charmers"},
			Write: []string{"charmers"},
		},
	},
}, {
	about:  "entity found, fully qualified url, few fields",
	stored: []string{"42 cs:~charmers/utopic/django-42", "~who/precise/django-47"},
	url:    "~who/precise/django-0",
	fields: []string{"public", "user"},
	expect: &mongodoc.BaseEntity{
		URL:    charm.MustParseURL("~who/django"),
		User:   "who",
		Public: false,
	},
}, {
	about:  "entity found, partial url, only the ACLs",
	stored: []string{"42 cs:~charmers/utopic/django-42", "~who/trusty/django-47"},
	url:    "~who/django-42",
	fields: []string{"acls"},
	expect: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		ACLs: mongodoc.ACL{
			Read:  []string{"who"},
			Write: []string{"who"},
		},
	},
}, {
	about:  "entity not found, charm name",
	stored: []string{"42 cs:~charmers/utopic/django-42", "~who/trusty/django-47"},
	url:    "rails",
}, {
	about:  "entity not found, user",
	stored: []string{"42 cs:~charmers/utopic/django-42", "~who/trusty/django-47"},
	url:    "~dalek/django",
	fields: []string{"acls"},
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
			c.Assert(baseEntity, jc.DeepEquals, test.expect)
		}

		// Remove all the entities from the store.
		_, err = store.DB.Entities().RemoveAll(nil)
		c.Assert(err, gc.IsNil)
		_, err = store.DB.BaseEntities().RemoveAll(nil)
		c.Assert(err, gc.IsNil)
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

	err = store.Publish(url, StableChannel)
	c.Assert(err, gc.ErrorMatches, "cannot index cs:~charmers/precise/wordpress-12 to ElasticSearch: .*")
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
	about: "empty bundle",
	data:  &charm.BundleData{},
}, {
	about: "no units",
	data: &charm.BundleData{
		Services: map[string]*charm.ServiceSpec{
			"django": {
				Charm:    "cs:utopic/django-0",
				NumUnits: 0,
			},
			"haproxy": {
				Charm:    "cs:trusty/haproxy-0",
				NumUnits: 0,
			},
		},
	},
}, {
	about: "a single unit",
	data: &charm.BundleData{
		Services: map[string]*charm.ServiceSpec{
			"django": {
				Charm:    "cs:trusty/django-42",
				NumUnits: 1,
			},
			"haproxy": {
				Charm:    "cs:trusty/haproxy-47",
				NumUnits: 0,
			},
		},
	},
	expectUnits: 1,
}, {
	about: "multiple units",
	data: &charm.BundleData{
		Services: map[string]*charm.ServiceSpec{
			"django": {
				Charm:    "cs:utopic/django-1",
				NumUnits: 1,
			},
			"haproxy": {
				Charm:    "cs:utopic/haproxy-2",
				NumUnits: 2,
			},
			"postgres": {
				Charm:    "cs:utopic/postgres-3",
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
		url := router.MustNewResolvedURL("cs:~charmers/bundle/django-0", -1)
		url.URL.Revision = i
		url.PromulgatedRevision = i

		// Add the bundle used for this test.
		err := store.AddBundle(&testingBundle{
			data: test.data,
		}, AddParams{
			URL:      url,
			BlobName: "blobName",
			BlobHash: fakeBlobHash,
			BlobSize: fakeBlobSize,
		})
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
		Services: map[string]*charm.ServiceSpec{
			"django": {
				Charm:    "cs:utopic/django-0",
				NumUnits: 0,
			},
			"haproxy": {
				Charm:    "cs:trusty/haproxy-0",
				NumUnits: 0,
			},
		},
	},
}, {
	about: "a single machine (no placement)",
	data: &charm.BundleData{
		Services: map[string]*charm.ServiceSpec{
			"django": {
				Charm:    "cs:trusty/django-42",
				NumUnits: 1,
			},
			"haproxy": {
				Charm:    "cs:trusty/haproxy-47",
				NumUnits: 0,
			},
		},
	},
	expectMachines: 1,
}, {
	about: "a single machine (machine placement)",
	data: &charm.BundleData{
		Services: map[string]*charm.ServiceSpec{
			"django": {
				Charm:    "cs:trusty/django-42",
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
		Services: map[string]*charm.ServiceSpec{
			"django": {
				Charm:    "cs:trusty/django-42",
				NumUnits: 1,
				To:       []string{"1"},
			},
			"haproxy": {
				Charm:    "cs:trusty/haproxy-47",
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
		Services: map[string]*charm.ServiceSpec{
			"django": {
				Charm:    "cs:trusty/django-42",
				NumUnits: 1,
			},
			"haproxy": {
				Charm:    "cs:trusty/haproxy-47",
				NumUnits: 1,
				To:       []string{"django/0"},
			},
		},
	},
	expectMachines: 1,
}, {
	about: "a single machine (containerization)",
	data: &charm.BundleData{
		Services: map[string]*charm.ServiceSpec{
			"django": {
				Charm:    "cs:trusty/django-42",
				NumUnits: 1,
				To:       []string{"1"},
			},
			"haproxy": {
				Charm:    "cs:trusty/haproxy-47",
				NumUnits: 1,
				To:       []string{"lxc:1"},
			},
			"postgres": {
				Charm:    "cs:utopic/postgres-3",
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
		Services: map[string]*charm.ServiceSpec{
			"django": {
				Charm:    "cs:utopic/django-1",
				NumUnits: 1,
			},
			"haproxy": {
				Charm:    "cs:utopic/haproxy-2",
				NumUnits: 2,
			},
			"postgres": {
				Charm:    "cs:utopic/postgres-3",
				NumUnits: 5,
			},
		},
	},
	expectMachines: 1 + 2 + 5,
}, {
	about: "multiple machines (machine placement)",
	data: &charm.BundleData{
		Services: map[string]*charm.ServiceSpec{
			"django": {
				Charm:    "cs:utopic/django-1",
				NumUnits: 2,
				To:       []string{"1", "3"},
			},
			"haproxy": {
				Charm:    "cs:utopic/haproxy-2",
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
		Services: map[string]*charm.ServiceSpec{
			"django": {
				Charm:    "cs:trusty/django-42",
				NumUnits: 1,
				To:       []string{"1"},
			},
			"haproxy": {
				Charm:    "cs:trusty/haproxy-47",
				NumUnits: 1,
				To:       []string{"2"},
			},
			"postgres": {
				Charm:    "cs:utopic/postgres-3",
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
		Services: map[string]*charm.ServiceSpec{
			"django": {
				Charm:    "cs:trusty/django-42",
				NumUnits: 2,
			},
			"haproxy": {
				Charm:    "cs:trusty/haproxy-47",
				NumUnits: 3,
				To:       []string{"django/0", "django/1", "new"},
			},
		},
	},
	expectMachines: 2 + 1,
}, {
	about: "multiple machines (containerization)",
	data: &charm.BundleData{
		Services: map[string]*charm.ServiceSpec{
			"django": {
				Charm:    "cs:trusty/django-42",
				NumUnits: 2,
				To:       []string{"1", "2"},
			},
			"haproxy": {
				Charm:    "cs:trusty/haproxy-47",
				NumUnits: 4,
				To:       []string{"lxc:1", "lxc:2", "lxc:3", "lxc:3"},
			},
			"postgres": {
				Charm:    "cs:utopic/postgres-3",
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
		Services: map[string]*charm.ServiceSpec{
			"django": {
				Charm:    "cs:trusty/django-42",
				NumUnits: 1,
				To:       []string{"1"},
			},
			"haproxy": {
				Charm:    "cs:trusty/haproxy-47",
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
		Services: map[string]*charm.ServiceSpec{
			"django": {
				Charm:    "cs:trusty/django-42",
				NumUnits: 1,
				To:       []string{"1"},
			},
			"haproxy": {
				Charm:    "cs:trusty/haproxy-47",
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
		Services: map[string]*charm.ServiceSpec{
			"django": {
				Charm:    "cs:trusty/django-42",
				NumUnits: 3,
			},
			"haproxy": {
				Charm:    "cs:trusty/haproxy-47",
				NumUnits: 6,
				To:       []string{"new", "1", "lxc:1", "new"},
			},
			"postgres": {
				Charm:    "cs:utopic/postgres-3",
				NumUnits: 10,
				To:       []string{"kvm:2", "lxc:django/1", "new", "new", "kvm:2"},
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
		Services: map[string]*charm.ServiceSpec{
			"haproxy": {
				Charm:    "cs:trusty/haproxy-47",
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
		url := router.MustNewResolvedURL("cs:~charmers/bundle/django-0", -1)
		url.URL.Revision = i
		url.PromulgatedRevision = i
		err := test.data.Verify(nil, nil)
		c.Assert(err, gc.IsNil)
		// Add the bundle used for this test.
		err = store.AddBundle(&testingBundle{
			data: test.data,
		}, AddParams{
			URL:      url,
			BlobName: "blobName",
			BlobHash: fakeBlobHash,
			BlobSize: fakeBlobSize,
		})
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
	return &router.ResolvedURL{
		URL:                 *url.WithChannel(""),
		PromulgatedRevision: promRev,
		Development:         url.Channel == charm.DevelopmentChannel,
	}
}

func MustParseResolvedURLs(urlStrs []string) []*router.ResolvedURL {
	urls := make([]*router.ResolvedURL, len(urlStrs))
	for i, u := range urlStrs {
		urls[i] = MustParseResolvedURL(u)
	}
	return urls
}

func (s *StoreSuite) TestAddPromulgatedCharmDir(c *gc.C) {
	charmDir := storetesting.Charms.CharmDir("wordpress")
	s.checkAddCharm(c, charmDir, false, router.MustNewResolvedURL("~charmers/precise/wordpress-1", 1))
}

func (s *StoreSuite) TestAddPromulgatedCharmArchive(c *gc.C) {
	charmArchive := storetesting.Charms.CharmArchive(c.MkDir(), "wordpress")
	s.checkAddCharm(c, charmArchive, false, router.MustNewResolvedURL("~charmers/precise/wordpress-1", 1))
}

func (s *StoreSuite) TestAddUserOwnedCharmDir(c *gc.C) {
	charmDir := storetesting.Charms.CharmDir("wordpress")
	s.checkAddCharm(c, charmDir, false, router.MustNewResolvedURL("~charmers/precise/wordpress-1", -1))
}

func (s *StoreSuite) TestAddUserOwnedCharmArchive(c *gc.C) {
	charmArchive := storetesting.Charms.CharmArchive(c.MkDir(), "wordpress")
	s.checkAddCharm(c, charmArchive, false, router.MustNewResolvedURL("~charmers/precise/wordpress-1", -1))
}

func (s *StoreSuite) TestAddBundleDir(c *gc.C) {
	bundleDir := storetesting.Charms.BundleDir("wordpress-simple")
	s.checkAddBundle(c, bundleDir, false, router.MustNewResolvedURL("~charmers/bundle/wordpress-simple-2", 3))
}

func (s *StoreSuite) TestAddBundleArchive(c *gc.C) {
	bundleArchive, err := charm.ReadBundleArchive(
		storetesting.Charms.BundleArchivePath(c.MkDir(), "wordpress-simple"),
	)
	c.Assert(err, gc.IsNil)
	s.checkAddBundle(c, bundleArchive, false, router.MustNewResolvedURL("~charmers/bundle/wordpress-simple-2", 3))
}

func (s *StoreSuite) TestAddUserOwnedBundleDir(c *gc.C) {
	bundleDir := storetesting.Charms.BundleDir("wordpress-simple")
	s.checkAddBundle(c, bundleDir, false, router.MustNewResolvedURL("~charmers/bundle/wordpress-simple-1", -1))
}

func (s *StoreSuite) TestAddUserOwnedBundleArchive(c *gc.C) {
	bundleArchive, err := charm.ReadBundleArchive(
		storetesting.Charms.BundleArchivePath(c.MkDir(), "wordpress-simple"),
	)
	c.Assert(err, gc.IsNil)
	s.checkAddBundle(c, bundleArchive, false, router.MustNewResolvedURL("~charmers/bundle/wordpress-simple-1", -1))
}

func (s *StoreSuite) newStore(c *gc.C, withES bool) *Store {
	var si *SearchIndex
	if withES {
		si = &SearchIndex{s.ES, s.TestIndex}
	}
	p, err := NewPool(s.Session.DB("juju_test"), si, &bakery.NewServiceParams{}, ServerParams{})
	c.Assert(err, gc.IsNil)
	store := p.Store()
	p.Close()
	return store
}

func (s *StoreSuite) TestAddCharmWithBundleSeries(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()
	ch := storetesting.Charms.CharmArchive(c.MkDir(), "wordpress")
	err := store.AddCharm(ch, AddParams{
		URL: router.MustNewResolvedURL("~charmers/bundle/wordpress-2", -1),
	})
	c.Assert(err, gc.ErrorMatches, `charm added with invalid id cs:~charmers/bundle/wordpress-2`)
}

func (s *StoreSuite) TestAddCharmWithMultiSeries(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()
	ch := storetesting.Charms.CharmArchive(c.MkDir(), "multi-series")
	s.checkAddCharm(c, ch, false, router.MustNewResolvedURL("~charmers/multi-series-1", 1))
	// Make sure it can be accessed with a number of names
	e, err := store.FindEntity(router.MustNewResolvedURL("~charmers/multi-series-1", 1), nil)
	c.Assert(err, gc.IsNil)
	c.Assert(e.URL.String(), gc.Equals, "cs:~charmers/multi-series-1")
	e, err = store.FindEntity(router.MustNewResolvedURL("~charmers/trusty/multi-series-1", 1), nil)
	c.Assert(err, gc.IsNil)
	c.Assert(e.URL.String(), gc.Equals, "cs:~charmers/multi-series-1")
	e, err = store.FindEntity(router.MustNewResolvedURL("~charmers/wily/multi-series-1", 1), nil)
	c.Assert(err, gc.IsNil)
	c.Assert(e.URL.String(), gc.Equals, "cs:~charmers/multi-series-1")
	_, err = store.FindEntity(router.MustNewResolvedURL("~charmers/precise/multi-series-1", 1), nil)
	c.Assert(err, gc.ErrorMatches, "entity not found")
	c.Assert(errgo.Cause(err), gc.Equals, params.ErrNotFound)
}

func (s *StoreSuite) TestAddCharmWithSeriesWhenThereIsAnExistingMultiSeriesVersion(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()
	ch := storetesting.Charms.CharmArchive(c.MkDir(), "multi-series")
	err := store.AddCharm(ch, AddParams{
		URL: router.MustNewResolvedURL("~charmers/multi-series-1", -1),
	})
	c.Assert(err, gc.IsNil)
	ch = storetesting.Charms.CharmArchive(c.MkDir(), "wordpress")
	err = store.AddCharm(ch, AddParams{
		URL: router.MustNewResolvedURL("~charmers/trusty/multi-series-2", -1),
	})
	c.Assert(err, gc.ErrorMatches, `charm name duplicates multi-series charm name cs:~charmers/multi-series-1`)
}

func (s *StoreSuite) TestAddCharmWithMultiSeriesToES(c *gc.C) {
	store := s.newStore(c, true)
	defer store.Close()
	ch := storetesting.Charms.CharmArchive(c.MkDir(), "multi-series")
	s.checkAddCharm(c, ch, true, router.MustNewResolvedURL("~charmers/juju-gui-1", 1))
}

var addInvalidCharmURLTests = []string{
	"cs:precise/wordpress-2",          // no user
	"cs:~charmers/precise/wordpress",  // no revision
	"cs:~charmers/bundle/wordpress-2", // invalid series
}

func (s *StoreSuite) TestAddInvalidCharmURL(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()
	ch := storetesting.Charms.CharmArchive(c.MkDir(), "wordpress")
	for i, urlStr := range addInvalidCharmURLTests {
		c.Logf("test %d: %s", i, urlStr)
		err := store.AddCharm(ch, AddParams{
			URL: &router.ResolvedURL{
				URL:                 *charm.MustParseURL(urlStr),
				PromulgatedRevision: -1,
			},
		})
		c.Assert(err, gc.ErrorMatches, `charm added with invalid id .*`)
	}
}

var addInvalidBundleURLTests = []string{
	"cs:bundle/wordpress-2",            // no user
	"cs:~charmers/bundle/wordpress",    // no revision
	"cs:~charmers/wordpress-2",         // no series
	"cs:~charmers/precise/wordpress-3", // invalid series
}

func (s *StoreSuite) TestAddBundleWithCharmSeries(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()
	b := storetesting.Charms.BundleDir("wordpress-simple")
	for i, urlStr := range addInvalidBundleURLTests {
		c.Logf("test %d: %s", i, urlStr)
		err := store.AddBundle(b, AddParams{
			URL: &router.ResolvedURL{
				URL:                 *charm.MustParseURL(urlStr),
				PromulgatedRevision: -1,
			},
		})
		c.Assert(err, gc.ErrorMatches, `bundle added with invalid id .*`)
	}
}

func (s *StoreSuite) TestAddBundleDuplicatingCharm(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()
	ch := storetesting.Charms.CharmDir("wordpress")
	err := store.AddCharmWithArchive(router.MustNewResolvedURL("~charmers/precise/wordpress-2", -1), ch)
	c.Assert(err, gc.IsNil)

	b := storetesting.Charms.BundleDir("wordpress-simple")
	err = store.AddBundleWithArchive(router.MustNewResolvedURL("~charmers/bundle/wordpress-5", -1), b)
	c.Assert(err, gc.ErrorMatches, "bundle name duplicates charm name cs:~charmers/precise/wordpress-2")
}

func (s *StoreSuite) TestAddCharmDuplicatingBundle(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()

	b := storetesting.Charms.BundleDir("wordpress-simple")
	err := store.AddBundleWithArchive(router.MustNewResolvedURL("~charmers/bundle/wordpress-2", -1), b)
	c.Assert(err, gc.IsNil)

	ch := storetesting.Charms.CharmDir("wordpress")
	err = store.AddCharmWithArchive(router.MustNewResolvedURL("~charmers/precise/wordpress-5", -1), ch)
	c.Assert(err, gc.ErrorMatches, "charm name duplicates bundle name cs:~charmers/bundle/wordpress-2")
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
	expectHash := hashOfReader(c, f)

	r, size, hash, err := store.OpenBlob(url)
	c.Assert(err, gc.IsNil)
	defer r.Close()

	c.Assert(hashOfReader(c, r), gc.Equals, expectHash)
	c.Assert(hash, gc.Equals, expectHash)

	info, err := f.Stat()
	c.Assert(err, gc.IsNil)
	c.Assert(size, gc.Equals, info.Size())
}

func (s *StoreSuite) TestBlobNameAndHash(c *gc.C) {
	charmArchive := storetesting.Charms.CharmArchive(c.MkDir(), "wordpress")

	store := s.newStore(c, false)
	defer store.Close()
	url := router.MustNewResolvedURL("cs:~charmers/precise/wordpress-23", 23)
	err := store.AddCharmWithArchive(url, charmArchive)
	c.Assert(err, gc.IsNil)

	f, err := os.Open(charmArchive.Path)
	c.Assert(err, gc.IsNil)
	defer f.Close()
	expectHash := hashOfReader(c, f)

	name, hash, err := store.BlobNameAndHash(url)
	c.Assert(err, gc.IsNil)

	r, _, err := store.BlobStore.Open(name)
	c.Assert(err, gc.IsNil)
	defer r.Close()

	c.Assert(hash, gc.Equals, expectHash)
	c.Assert(hashOfReader(c, r), gc.Equals, expectHash)
}

func (s *StoreSuite) TestAddLog(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()
	urls := []*charm.URL{
		charm.MustParseURL("cs:django"),
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
		charm.MustParseURL("trusty/django-42"),
		charm.MustParseURL("~who/utopic/wordpress"),
	})
	c.Assert(err, gc.IsNil)

	// Retrieve the log from the store.
	var doc mongodoc.Log
	err = store.DB.Logs().Find(nil).One(&doc)
	c.Assert(err, gc.IsNil)

	// The log includes the base URLs.
	c.Assert(doc.URLs, jc.DeepEquals, []*charm.URL{
		charm.MustParseURL("trusty/django-42"),
		charm.MustParseURL("django"),
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
		charm.MustParseURL("trusty/django-42"),
		charm.MustParseURL("django"),
		charm.MustParseURL("trusty/django-42"),
		charm.MustParseURL("django"),
	})
	c.Assert(err, gc.IsNil)

	// Retrieve the log from the store.
	var doc mongodoc.Log
	err = store.DB.Logs().Find(nil).One(&doc)
	c.Assert(err, gc.IsNil)

	// The log excludes duplicate URLs.
	c.Assert(doc.URLs, jc.DeepEquals, []*charm.URL{
		charm.MustParseURL("trusty/django-42"),
		charm.MustParseURL("django"),
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

func (s *StoreSuite) TestAddCharmWithUser(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()

	wordpress := storetesting.Charms.CharmDir("wordpress")
	url := router.MustNewResolvedURL("cs:~who/precise/wordpress-23", -1)
	err := store.AddCharmWithArchive(url, wordpress)
	c.Assert(err, gc.IsNil)
	entity, err := store.FindEntity(url, nil)
	c.Assert(err, gc.IsNil)
	assertBaseEntity(c, store, entity, false)
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

func hashOfReader(c *gc.C, r io.Reader) string {
	hash := sha512.New384()
	_, err := io.Copy(hash, r)
	c.Assert(err, gc.IsNil)
	return fmt.Sprintf("%x", hash.Sum(nil))
}

func getSizeAndHashes(c interface{}) (int64, string, string) {
	var r io.ReadWriter
	var err error
	switch c := c.(type) {
	case archiverTo:
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

// Define fake blob attributes to be used in tests.
var fakeBlobSize, fakeBlobHash = func() (int64, string) {
	b := []byte("fake content")
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

func (s *StoreSuite) TestAddCharmDirIndexed(c *gc.C) {
	charmDir := storetesting.Charms.CharmDir("wordpress")
	s.checkAddCharm(c, charmDir, true, router.MustNewResolvedURL("cs:~charmers/precise/wordpress-2", -1))
}

func (s *StoreSuite) TestAddCharmArchiveIndexed(c *gc.C) {
	charmArchive := storetesting.Charms.CharmArchive(c.MkDir(), "wordpress")
	s.checkAddCharm(c, charmArchive, true, router.MustNewResolvedURL("cs:~charmers/precise/wordpress-2", -1))
}

func (s *StoreSuite) TestAddBundleDirIndexed(c *gc.C) {
	bundleDir := storetesting.Charms.BundleDir("wordpress-simple")
	s.checkAddBundle(c, bundleDir, true, router.MustNewResolvedURL("cs:~charmers/bundle/baboom-2", -1))
}

func (s *StoreSuite) TestAddBundleArchiveIndexed(c *gc.C) {
	bundleArchive, err := charm.ReadBundleArchive(
		storetesting.Charms.BundleArchivePath(c.MkDir(), "wordpress-simple"),
	)
	c.Assert(err, gc.IsNil)
	s.checkAddBundle(c, bundleArchive, true, router.MustNewResolvedURL("cs:~charmers/bundle/baboom-2", -1))
}

func (s *StoreSuite) TestAddCharmDirIndexedAndPromulgated(c *gc.C) {
	charmDir := storetesting.Charms.CharmDir("wordpress")
	s.checkAddCharm(c, charmDir, true, router.MustNewResolvedURL("cs:~charmers/precise/wordpress-2", -1))
}

func (s *StoreSuite) TestAddCharmArchiveIndexedAndPromulgated(c *gc.C) {
	charmArchive := storetesting.Charms.CharmArchive(c.MkDir(), "wordpress")
	s.checkAddCharm(c, charmArchive, true, router.MustNewResolvedURL("cs:~charmers/precise/wordpress-2", 2))
}

func (s *StoreSuite) TestAddBundleDirIndexedAndPromulgated(c *gc.C) {
	bundleDir := storetesting.Charms.BundleDir("wordpress-simple")
	s.checkAddBundle(c, bundleDir, true, router.MustNewResolvedURL("cs:~charmers/bundle/baboom-2", 2))
}

func (s *StoreSuite) TestAddBundleArchiveIndexedAndPromulgated(c *gc.C) {
	bundleArchive, err := charm.ReadBundleArchive(
		storetesting.Charms.BundleArchivePath(c.MkDir(), "wordpress-simple"),
	)
	c.Assert(err, gc.IsNil)
	s.checkAddBundle(c, bundleArchive, true, router.MustNewResolvedURL("cs:~charmers/bundle/baboom-2", 2))
}

var findBestEntityTests = []struct {
	url       string
	expectURL string
	expectErr string
}{{
	url:       "~charmers/trusty/wordpress-10",
	expectURL: "~charmers/trusty/wordpress-10",
}, {
	url:       "~charmers/trusty/wordpress",
	expectURL: "~charmers/trusty/wordpress-12",
}, {
	url:       "trusty/wordpress-11",
	expectURL: "~charmers/trusty/wordpress-11",
}, {
	url:       "trusty/wordpress",
	expectURL: "~mickey/trusty/wordpress-13",
}, {
	url:       "wordpress",
	expectURL: "~mickey/trusty/wordpress-13",
}, {
	url:       "~mickey/wordpress-12",
	expectErr: "entity not found",
}, {
	url:       "~mickey/precise/wordpress",
	expectURL: "~mickey/precise/wordpress-24",
}, {
	url:       "mysql",
	expectErr: "entity not found",
}, {
	url:       "precise/wordpress",
	expectURL: "~mickey/precise/wordpress-24",
}, {
	url:       "~donald/bundle/wordpress-simple-0",
	expectURL: "~donald/bundle/wordpress-simple-0",
}, {
	url:       "~donald/bundle/wordpress-simple",
	expectURL: "~donald/bundle/wordpress-simple-1",
}, {
	url:       "~donald/wordpress-simple-0",
	expectURL: "~donald/bundle/wordpress-simple-0",
}, {
	url:       "bundle/wordpress-simple-0",
	expectURL: "~donald/bundle/wordpress-simple-1",
}, {
	url:       "bundle/wordpress-simple",
	expectURL: "~donald/bundle/wordpress-simple-1",
}, {
	url:       "wordpress-simple",
	expectURL: "~donald/bundle/wordpress-simple-1",
}, {
	url:       "~pluto/multi-series",
	expectURL: "~pluto/wily/multi-series-1",
}}

func (s *StoreSuite) TestFindBestEntity(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()

	// Add entities to the database.
	entities := []*mongodoc.Entity{{
		URL:            charm.MustParseURL("~charmers/trusty/wordpress-9"),
		PromulgatedURL: charm.MustParseURL("trusty/wordpress-9"),
	}, {
		URL:            charm.MustParseURL("~charmers/trusty/wordpress-10"),
		PromulgatedURL: charm.MustParseURL("trusty/wordpress-10"),
	}, {
		URL:            charm.MustParseURL("~charmers/trusty/wordpress-11"),
		PromulgatedURL: charm.MustParseURL("trusty/wordpress-11"),
	}, {
		URL:            charm.MustParseURL("~charmers/trusty/wordpress-12"),
		PromulgatedURL: charm.MustParseURL("trusty/wordpress-12"),
	}, {
		URL:            charm.MustParseURL("~charmers/trusty/wordpress-13"),
		PromulgatedURL: charm.MustParseURL("trusty/wordpress-13"),
	}, {
		URL: charm.MustParseURL("~mickey/precise/wordpress-12"),
	}, {
		URL: charm.MustParseURL("~mickey/trusty/wordpress-12"),
	}, {
		URL:            charm.MustParseURL("~mickey/trusty/wordpress-13"),
		PromulgatedURL: charm.MustParseURL("trusty/wordpress-14"),
	}, {
		URL:            charm.MustParseURL("~mickey/precise/wordpress-24"),
		PromulgatedURL: charm.MustParseURL("precise/wordpress-24"),
	}, {
		URL: charm.MustParseURL("~donald/bundle/wordpress-simple-0"),
	}, {
		URL:            charm.MustParseURL("~donald/bundle/wordpress-simple-1"),
		PromulgatedURL: charm.MustParseURL("bundle/wordpress-simple-0"),
	}, {
		URL: charm.MustParseURL("~pluto/utopic/multi-series-2"),
	}, {
		URL: charm.MustParseURL("~pluto/wily/multi-series-1"),
	}}
	// TODO add development entities above.
	for _, e := range entities {
		err := store.DB.Entities().Insert(denormalizedEntity(e))
		c.Assert(err, gc.IsNil)
	}

	// Add base entities to the database.
	baseEntities := []*mongodoc.BaseEntity{{
		URL: charm.MustParseURL("~charmers/wordpress"),
		StableSeries: map[string]*charm.URL{
			"trusty": charm.MustParseURL("~charmers/trusty/wordpress-12"),
		},
		Name: "wordpress",
	}, {
		URL: charm.MustParseURL("~mickey/wordpress"),
		StableSeries: map[string]*charm.URL{
			"precise": charm.MustParseURL("~mickey/precise/wordpress-24"),
			"trusty":  charm.MustParseURL("~mickey/trusty/wordpress-13"),
		},
		Name:        "wordpress",
		Promulgated: true,
	}, {
		URL: charm.MustParseURL("~donald/wordpress-simple"),
		StableSeries: map[string]*charm.URL{
			"bundle": charm.MustParseURL("~donald/bundle/wordpress-simple-1"),
		},
		Name:        "wordpress-simple",
		Promulgated: true,
	}, {
		URL: charm.MustParseURL("~pluto/multi-series"),
		StableSeries: map[string]*charm.URL{
			"utopic": charm.MustParseURL("~pluto/utopic/multi-series-2"),
			"wily":   charm.MustParseURL("~pluto/wily/multi-series-1"),
		},
		Name: "multi-series",
	}}
	for _, e := range baseEntities {
		err := store.DB.BaseEntities().Insert(e)
		c.Assert(err, gc.IsNil)
	}

	// Execute the tests.
	for i, test := range findBestEntityTests {
		c.Logf("test %d: %s", i, test.url)
		entity, err := store.FindBestEntity(charm.MustParseURL(test.url), nil)
		if test.expectErr != "" {
			c.Assert(err, gc.ErrorMatches, test.expectErr)
		} else {
			c.Assert(err, gc.IsNil)
			c.Assert(entity.URL.String(), gc.Equals, charm.MustParseURL(test.expectURL).String())
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
		Stable:                  true,
		Development:             true,
		CharmProvidedInterfaces: []string{"a", "b"},
		CharmRequiredInterfaces: []string{"b", "c"},
	}, {
		URL:                     charm.MustParseURL("~charmers/trusty/wordpress-2"),
		PromulgatedURL:          charm.MustParseURL("trusty/wordpress-2"),
		Stable:                  true,
		CharmProvidedInterfaces: []string{"a", "b"},
		CharmRequiredInterfaces: []string{"b", "c", "x"},
	}, {
		// Note: development only charm should never be found.
		URL:                     charm.MustParseURL("~charmers/trusty/wordpress-3"),
		PromulgatedURL:          charm.MustParseURL("trusty/wordpress-3"),
		Development:             true,
		CharmProvidedInterfaces: []string{"a", "b"},
		CharmRequiredInterfaces: []string{"b", "c", "x"},
	}, {
		URL:                     charm.MustParseURL("~charmers/trusty/mysql-1"),
		PromulgatedURL:          charm.MustParseURL("trusty/mysql-1"),
		Stable:                  true,
		CharmProvidedInterfaces: []string{"d", "b"},
		CharmRequiredInterfaces: []string{"e", "x"},
	}, {
		// Unpublished charm should never be found.
		URL:                     charm.MustParseURL("~charmers/trusty/mysql-2"),
		PromulgatedURL:          charm.MustParseURL("trusty/mysql-2"),
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

var findBestEntityWithMultiSeriesCharmsTests = []struct {
	about        string
	entities     []*mongodoc.Entity
	baseEntities []*mongodoc.BaseEntity
	url          string
	expectURL    string
}{{
	about: "URL with series and revision can select multi-series charm",
	entities: []*mongodoc.Entity{{
		URL:             charm.MustParseURL("~charmers/wordpress-10"),
		SupportedSeries: []string{"precise", "trusty"},
	}},
	url:       "~charmers/trusty/wordpress-10",
	expectURL: "~charmers/wordpress-10",
}, {
	about: "URL with series and revision gives not found if series not supported",
	entities: []*mongodoc.Entity{{
		URL:             charm.MustParseURL("~charmers/wordpress-10"),
		SupportedSeries: []string{"trusty"},
	}, {
		URL:             charm.MustParseURL("~bob/wordpress-12"),
		SupportedSeries: []string{"quantal"},
	}},
	url: "~charmers/utopic/wordpress-10",
}, {
	about: "URL with series and no revision prefers current stable revision for that series",
	entities: []*mongodoc.Entity{{
		URL:             charm.MustParseURL("~charmers/wordpress-10"),
		SupportedSeries: []string{"precise", "trusty"},
	}, {
		URL:             charm.MustParseURL("~charmers/wordpress-11"),
		SupportedSeries: []string{"quantal"},
	}, {
		URL:             charm.MustParseURL("~charmers/wordpress-12"),
		SupportedSeries: []string{"precise"},
	}, {
		URL:             charm.MustParseURL("~charmers/wordpress-13"),
		SupportedSeries: []string{"precise"},
	}, {
		URL:             charm.MustParseURL("~charmers/wordpress-14"),
		SupportedSeries: []string{"trusty"},
	}, {
		URL:             charm.MustParseURL("~bob/wordpress-15"),
		SupportedSeries: []string{"precise"},
	}},
	baseEntities: []*mongodoc.BaseEntity{{
		URL: charm.MustParseURL("~charmers/wordpress"),
		StableSeries: map[string]*charm.URL{
			"precise": charm.MustParseURL("~charmers/wordpress-13"),
			"quantal": charm.MustParseURL("~charmers/wordpress-11"),
		},
	}},
	url:       "~charmers/precise/wordpress",
	expectURL: "~charmers/wordpress-13",
}, {
	about: "development URL with series and no revision prefers current development revision for that series",
	entities: []*mongodoc.Entity{{
		URL:             charm.MustParseURL("~charmers/wordpress-10"),
		SupportedSeries: []string{"precise", "trusty"},
		Development:     true,
	}, {
		URL:             charm.MustParseURL("~charmers/wordpress-11"),
		SupportedSeries: []string{"quantal"},
		Development:     true,
	}, {
		URL:             charm.MustParseURL("~charmers/wordpress-12"),
		SupportedSeries: []string{"precise"},
		Development:     true,
	}, {
		URL:             charm.MustParseURL("~charmers/wordpress-13"),
		SupportedSeries: []string{"precise"},
		Development:     true,
	}, {
		URL:             charm.MustParseURL("~charmers/wordpress-14"),
		SupportedSeries: []string{"trusty"},
		Development:     true,
	}, {
		URL:             charm.MustParseURL("~bob/wordpress-15"),
		SupportedSeries: []string{"precise"},
		Development:     true,
	}},
	baseEntities: []*mongodoc.BaseEntity{{
		URL: charm.MustParseURL("~charmers/wordpress"),
		StableSeries: map[string]*charm.URL{
			"precise": charm.MustParseURL("~charmers/wordpress-13"),
			"quantal": charm.MustParseURL("~charmers/wordpress-11"),
		},
		DevelopmentSeries: map[string]*charm.URL{
			"precise": charm.MustParseURL("~charmers/wordpress-14"),
			"quantal": charm.MustParseURL("~charmers/wordpress-11"),
		},
	}},
	url:       "~charmers/development/precise/wordpress",
	expectURL: "~charmers/wordpress-14",
}, {
	about: "URL with no series and revision resolves to the given exact entity",
	entities: []*mongodoc.Entity{{
		URL:             charm.MustParseURL("~charmers/wordpress-10"),
		SupportedSeries: []string{"precise", "trusty"},
	}},
	url:       "~charmers/wordpress-10",
	expectURL: "~charmers/wordpress-10",
}, {
	about: "URL with no series and revision will not find non-multi-series charm",
	entities: []*mongodoc.Entity{{
		URL: charm.MustParseURL("~charmers/precise/wordpress-10"),
	}},
	url: "~charmers/wordpress-10",
}, {
	about: "URL with no series and revision can find bundle",
	entities: []*mongodoc.Entity{{
		URL: charm.MustParseURL("~charmers/bundle/trundle-10"),
	}},
	url:       "~charmers/trundle-10",
	expectURL: "~charmers/bundle/trundle-10",
}, {
	about: "URL with no series and no revision finds best multi-series charm in base entity",
	entities: []*mongodoc.Entity{{
		URL:             charm.MustParseURL("~charmers/wordpress-14"),
		SupportedSeries: []string{"precise", "wily"},
	}, {
		URL:             charm.MustParseURL("~charmers/wordpress-11"),
		SupportedSeries: []string{"precise", "trusty"},
	}, {
		URL:             charm.MustParseURL("~charmers/wordpress-10"),
		SupportedSeries: []string{"precise", "saucy"},
	}, {
		URL:             charm.MustParseURL("~charmers/wordpress-12"),
		SupportedSeries: []string{"precise"},
	}},
	baseEntities: []*mongodoc.BaseEntity{{
		URL: charm.MustParseURL("~charmers/wordpress"),
		StableSeries: map[string]*charm.URL{
			"precise": charm.MustParseURL("~charmers/wordpress-12"),
			"saucy":   charm.MustParseURL("~charmers/wordpress-10"),
			"trusty":  charm.MustParseURL("~charmers/wordpress-11"),
			"wily":    charm.MustParseURL("~charmers/wordpress-14"),
		},
	}},
	url:       "~charmers/wordpress",
	expectURL: "~charmers/wordpress-11",
}, {
	about: "development URL with no series and no revision finds best development multi-series charm in base entity",
	entities: []*mongodoc.Entity{{
		URL:             charm.MustParseURL("~charmers/wordpress-14"),
		SupportedSeries: []string{"precise", "wily"},
		Development:     true,
	}, {
		URL:             charm.MustParseURL("~charmers/wordpress-10"),
		SupportedSeries: []string{"precise", "saucy"},
		Development:     true,
	}, {
		URL:             charm.MustParseURL("~charmers/wordpress-12"),
		SupportedSeries: []string{"precise"},
		Development:     true,
	}},
	baseEntities: []*mongodoc.BaseEntity{{
		URL: charm.MustParseURL("~charmers/wordpress"),
		DevelopmentSeries: map[string]*charm.URL{
			"precise": charm.MustParseURL("~charmers/wordpress-12"),
			"saucy":   charm.MustParseURL("~charmers/wordpress-10"),
			"wily":    charm.MustParseURL("~charmers/wordpress-14"),
		},
	}},
	url:       "~charmers/development/wordpress",
	expectURL: "~charmers/wordpress-12",
}, {
	about: "promulgated URL with series, name and revision can select multi-series charm",
	entities: []*mongodoc.Entity{{
		URL:             charm.MustParseURL("~charmers/wordpress-10"),
		PromulgatedURL:  charm.MustParseURL("wordpress-2"),
		SupportedSeries: []string{"precise", "trusty"},
	}},
	url:       "precise/wordpress-2",
	expectURL: "~charmers/wordpress-10",
}, {
	about: "promulgated URL with series and no revision prefers best promulgated base entity revision that supports that series",
	entities: []*mongodoc.Entity{{
		URL:             charm.MustParseURL("~charmers/wordpress-10"),
		PromulgatedURL:  charm.MustParseURL("wordpress-1"),
		SupportedSeries: []string{"precise", "trusty"},
	}, {
		URL:             charm.MustParseURL("~charmers/wordpress-11"),
		PromulgatedURL:  charm.MustParseURL("wordpress-2"),
		SupportedSeries: []string{"quantal"},
	}, {
		URL:             charm.MustParseURL("~newcharmers/wordpress-1"),
		PromulgatedURL:  charm.MustParseURL("wordpress-3"),
		SupportedSeries: []string{"precise"},
	}, {
		URL:             charm.MustParseURL("~newcharmers/wordpress-13"),
		PromulgatedURL:  charm.MustParseURL("wordpress-4"),
		SupportedSeries: []string{"trusty"},
	}, {
		URL:             charm.MustParseURL("~bob/wordpress-14"),
		SupportedSeries: []string{"precise"},
	}},
	baseEntities: []*mongodoc.BaseEntity{{
		URL: charm.MustParseURL("~newcharmers/wordpress"),
		StableSeries: map[string]*charm.URL{
			"precise": charm.MustParseURL("~newcharmers/wordpress-1"),
			"trusty":  charm.MustParseURL("~newcharmers/wordpress-13"),
		},
		Name:        "wordpress",
		Promulgated: true,
	}},
	url:       "precise/wordpress",
	expectURL: "~newcharmers/wordpress-1",
}, {
	about: "promulgated URL with no series and revision resolves to the given exact entity",
	entities: []*mongodoc.Entity{{
		URL:             charm.MustParseURL("~charmers/wordpress-10"),
		PromulgatedURL:  charm.MustParseURL("wordpress-3"),
		SupportedSeries: []string{"precise", "trusty"},
	}},
	url:       "wordpress-3",
	expectURL: "~charmers/wordpress-10",
}, {
	about: "promulgated URL with no series and revision will not find non-multi-series charm",
	entities: []*mongodoc.Entity{{
		URL:            charm.MustParseURL("~charmers/precise/wordpress-10"),
		PromulgatedURL: charm.MustParseURL("precise/wordpress-3"),
	}},
	url: "wordpress-3",
}, {
	about: "promulgated URL with no series and revision can find bundle",
	entities: []*mongodoc.Entity{{
		URL:            charm.MustParseURL("~charmers/bundle/trundle-10"),
		PromulgatedURL: charm.MustParseURL("bundle/trundle-10"),
	}},
	url:       "trundle-10",
	expectURL: "~charmers/bundle/trundle-10",
}, {
	about: "promulgated URL with no series and no revision finds best multi-series charm in promulgated base entity",
	entities: []*mongodoc.Entity{{
		URL:             charm.MustParseURL("~charmers/wordpress-10"),
		PromulgatedURL:  charm.MustParseURL("wordpress-1"),
		SupportedSeries: []string{"precise", "trusty"},
	}, {
		URL:             charm.MustParseURL("~charmers/wordpress-11"),
		PromulgatedURL:  charm.MustParseURL("wordpress-2"),
		SupportedSeries: []string{"quantal"},
	}, {
		URL:             charm.MustParseURL("~newcharmers/wordpress-1"),
		PromulgatedURL:  charm.MustParseURL("wordpress-3"),
		SupportedSeries: []string{"precise"},
	}, {
		URL:             charm.MustParseURL("~newcharmers/wordpress-13"),
		PromulgatedURL:  charm.MustParseURL("wordpress-4"),
		SupportedSeries: []string{"trusty"},
	}, {
		URL:             charm.MustParseURL("~bob/wordpress-14"),
		SupportedSeries: []string{"precise"},
	}},
	baseEntities: []*mongodoc.BaseEntity{{
		URL: charm.MustParseURL("~newcharmers/wordpress"),
		StableSeries: map[string]*charm.URL{
			"precise": charm.MustParseURL("~newcharmers/wordpress-1"),
			"trusty":  charm.MustParseURL("~charmers/wordpress-10"),
		},
		Name:        "wordpress",
		Promulgated: true,
	}},
	url:       "wordpress",
	expectURL: "~charmers/wordpress-10",
}, {
	about: "promulgated URL in development with no series and no revision finds best multi-series charm in promulgated base entity",
	entities: []*mongodoc.Entity{{
		URL:             charm.MustParseURL("~charmers/wordpress-10"),
		PromulgatedURL:  charm.MustParseURL("wordpress-1"),
		SupportedSeries: []string{"precise", "trusty"},
		Development:     true,
	}, {
		URL:             charm.MustParseURL("~charmers/wordpress-11"),
		PromulgatedURL:  charm.MustParseURL("wordpress-2"),
		SupportedSeries: []string{"quantal"},
		Development:     true,
	}, {
		URL:             charm.MustParseURL("~newcharmers/wordpress-1"),
		PromulgatedURL:  charm.MustParseURL("wordpress-3"),
		SupportedSeries: []string{"precise"},
		Development:     true,
	}, {
		URL:             charm.MustParseURL("~newcharmers/wordpress-13"),
		PromulgatedURL:  charm.MustParseURL("wordpress-4"),
		SupportedSeries: []string{"trusty"},
		Development:     true,
	}, {
		URL:             charm.MustParseURL("~bob/wordpress-14"),
		SupportedSeries: []string{"precise"},
		Development:     true,
	}},
	baseEntities: []*mongodoc.BaseEntity{{
		URL: charm.MustParseURL("~newcharmers/wordpress"),
		DevelopmentSeries: map[string]*charm.URL{
			"precise": charm.MustParseURL("~newcharmers/wordpress-1"),
			"trusty":  charm.MustParseURL("~newcharmers/wordpress-13"),
		},
		Name:        "wordpress",
		Promulgated: true,
	}},
	url:       "development/wordpress",
	expectURL: "~newcharmers/wordpress-13",
}, {
	about: "URL with series, no revision and no stable releases will not find non-multi-series charm",
	entities: []*mongodoc.Entity{{
		URL:             charm.MustParseURL("~charmers/wordpress-10"),
		SupportedSeries: []string{"trusty", "precise"},
	}, {
		URL:             charm.MustParseURL("~charmers/wordpress-11"),
		SupportedSeries: []string{"trusty"},
	}},
	baseEntities: []*mongodoc.BaseEntity{{
		URL: charm.MustParseURL("~charmers/wordpress"),
		StableSeries: map[string]*charm.URL{
			"precise": charm.MustParseURL("~charmers/wordpress-10"),
		},
	}},
	url: "~charmers/trusty/wordpress",
}, {
	about: "URL with no series, no revision and no stable releases will not find non-multi-series charm",
	entities: []*mongodoc.Entity{{
		URL:             charm.MustParseURL("~charmers/wordpress-10"),
		SupportedSeries: []string{"trusty", "precise"},
	}, {
		URL:             charm.MustParseURL("~charmers/wordpress-11"),
		SupportedSeries: []string{"trusty"},
	}},
	baseEntities: []*mongodoc.BaseEntity{{
		URL: charm.MustParseURL("~charmers/wordpress"),
	}},
	url: "~charmers/wordpress",
}}

func (s *StoreSuite) TestFindBestEntityWithMultiSeriesCharms(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()

	for i, test := range findBestEntityWithMultiSeriesCharmsTests {
		c.Logf("test %d: %s", i, test.about)
		_, err := store.DB.Entities().RemoveAll(nil)
		c.Assert(err, gc.IsNil)
		_, err = store.DB.BaseEntities().RemoveAll(nil)
		c.Assert(err, gc.IsNil)
		for _, e := range test.entities {
			err := store.DB.Entities().Insert(denormalizedEntity(e))
			c.Assert(err, gc.IsNil)
		}
		for _, e := range test.baseEntities {
			err := store.DB.BaseEntities().Insert(e)
			c.Assert(err, gc.IsNil)
		}
		entity, err := store.FindBestEntity(charm.MustParseURL(test.url), nil)
		if test.expectURL == "" {
			c.Assert(errgo.Cause(err), gc.Equals, params.ErrNotFound)
		} else {
			c.Assert(err, gc.IsNil)
			c.Assert(entity.URL.String(), gc.Equals, charm.MustParseURL(test.expectURL).String())
		}
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
		err = store.UpdateBaseEntity(url, bson.D{{"$set", bson.D{{"acls", mongodoc.ACL{
			Read: []string{"test"},
		}}}}})
		if test.expectErr != "" {
			c.Assert(err, gc.ErrorMatches, test.expectErr)
		} else {
			c.Assert(err, gc.IsNil)
			baseEntity, err := store.FindBaseEntity(&url.URL, nil)
			c.Assert(err, gc.IsNil)
			c.Assert(baseEntity.ACLs.Read, jc.DeepEquals, []string{"test"})
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
		c.Logf("test %d. %s", i, test.about)
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
		for _, expectEntity := range test.expectEntities {
			entity, err := store.FindEntity(EntityResolvedURL(expectEntity), nil)
			c.Assert(err, gc.IsNil)
			c.Assert(entity, jc.DeepEquals, expectEntity)
		}
		for _, expectBaseEntity := range test.expectBaseEntities {
			baseEntity, err := store.FindBaseEntity(expectBaseEntity.URL, nil)
			c.Assert(err, gc.IsNil)
			c.Assert(baseEntity, jc.DeepEquals, expectBaseEntity)
		}
	}
}

func (s *StoreSuite) TestSetPromulgatedUpdateSearch(c *gc.C) {
	store := s.newStore(c, true)
	defer store.Close()

	// Insert some entities in the store, ensure there are a number of revisions of the same charm.
	err := store.DB.Entities().Insert(entity("~charmers/trusty/wordpress-0", "trusty/wordpress-2"))
	c.Assert(err, gc.IsNil)
	err = store.DB.Entities().Insert(entity("~charmers/precise/wordpress-0", "precise/wordpress-1"))
	c.Assert(err, gc.IsNil)
	err = store.DB.Entities().Insert(entity("~openstack-charmers/trusty/wordpress-0", ""))
	c.Assert(err, gc.IsNil)
	err = store.DB.Entities().Insert(entity("~openstack-charmers/precise/wordpress-0", ""))
	c.Assert(err, gc.IsNil)
	err = store.DB.BaseEntities().Insert(baseEntity("~charmers/wordpress", true))
	c.Assert(err, gc.IsNil)
	err = store.DB.BaseEntities().Insert(baseEntity("~openstack-charmers/wordpress", false))
	c.Assert(err, gc.IsNil)
	url := router.MustNewResolvedURL("~openstack-charmers/trusty/wordpress-0", -1)

	// Change the promulgated mysql version to openstack-charmers.
	err = store.SetPromulgated(url, true)
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

	// Remove the promulgated flag from openstack-charmers, meaning mysql is
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

var publishTests = []struct {
	about              string
	url                *router.ResolvedURL
	channels           []charm.Channel
	initialEntity      *mongodoc.Entity
	initialBaseEntity  *mongodoc.BaseEntity
	expectedEntity     *mongodoc.Entity
	expectedBaseEntity *mongodoc.BaseEntity
	expectedErr        string
}{{
	about:    "unpublished, single series, publish development",
	url:      MustParseResolvedURL("~who/trusty/django-42"),
	channels: []charm.Channel{charm.DevelopmentChannel},
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
		DevelopmentSeries: map[string]*charm.URL{
			"trusty": charm.MustParseURL("~who/trusty/django-42"),
		},
	},
}, {
	about:    "development, single series, publish development",
	url:      MustParseResolvedURL("~who/trusty/django-42"),
	channels: []charm.Channel{charm.DevelopmentChannel},
	initialEntity: &mongodoc.Entity{
		URL:         charm.MustParseURL("~who/trusty/django-42"),
		Development: true,
	},
	initialBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		DevelopmentSeries: map[string]*charm.URL{
			"trusty": charm.MustParseURL("~who/trusty/django-41"),
		},
	},
	expectedEntity: &mongodoc.Entity{
		URL:         charm.MustParseURL("~who/trusty/django-42"),
		Development: true,
	},
	expectedBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		DevelopmentSeries: map[string]*charm.URL{
			"trusty": charm.MustParseURL("~who/trusty/django-42"),
		},
	},
}, {
	about:    "stable, single series, publish development",
	url:      MustParseResolvedURL("~who/trusty/django-42"),
	channels: []charm.Channel{charm.DevelopmentChannel},
	initialEntity: &mongodoc.Entity{
		URL:    charm.MustParseURL("~who/trusty/django-42"),
		Stable: true,
	},
	initialBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		StableSeries: map[string]*charm.URL{
			"trusty": charm.MustParseURL("~who/trusty/django-42"),
		},
	},
	expectedEntity: &mongodoc.Entity{
		URL:         charm.MustParseURL("~who/trusty/django-42"),
		Stable:      true,
		Development: true,
	},
	expectedBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		StableSeries: map[string]*charm.URL{
			"trusty": charm.MustParseURL("~who/trusty/django-42"),
		},
		DevelopmentSeries: map[string]*charm.URL{
			"trusty": charm.MustParseURL("~who/trusty/django-42"),
		},
	},
}, {
	about:    "unpublished, single series, publish stable",
	url:      MustParseResolvedURL("~who/trusty/django-42"),
	channels: []charm.Channel{StableChannel},
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
		StableSeries: map[string]*charm.URL{
			"trusty": charm.MustParseURL("~who/trusty/django-42"),
		},
	},
}, {
	about:    "development, single series, publish stable",
	url:      MustParseResolvedURL("~who/trusty/django-42"),
	channels: []charm.Channel{StableChannel},
	initialEntity: &mongodoc.Entity{
		URL:         charm.MustParseURL("~who/trusty/django-42"),
		Development: true,
	},
	initialBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		DevelopmentSeries: map[string]*charm.URL{
			"trusty": charm.MustParseURL("~who/trusty/django-41"),
		},
	},
	expectedEntity: &mongodoc.Entity{
		URL:         charm.MustParseURL("~who/trusty/django-42"),
		Development: true,
		Stable:      true,
	},
	expectedBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		DevelopmentSeries: map[string]*charm.URL{
			"trusty": charm.MustParseURL("~who/trusty/django-41"),
		},
		StableSeries: map[string]*charm.URL{
			"trusty": charm.MustParseURL("~who/trusty/django-42"),
		},
	},
}, {
	about:    "stable, single series, publish stable",
	url:      MustParseResolvedURL("~who/trusty/django-42"),
	channels: []charm.Channel{StableChannel},
	initialEntity: &mongodoc.Entity{
		URL:    charm.MustParseURL("~who/trusty/django-42"),
		Stable: true,
	},
	initialBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		StableSeries: map[string]*charm.URL{
			"trusty": charm.MustParseURL("~who/trusty/django-40"),
		},
	},
	expectedEntity: &mongodoc.Entity{
		URL:    charm.MustParseURL("~who/trusty/django-42"),
		Stable: true,
	},
	expectedBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		StableSeries: map[string]*charm.URL{
			"trusty": charm.MustParseURL("~who/trusty/django-42"),
		},
	},
}, {
	about:    "unpublished, multi series, publish development",
	url:      MustParseResolvedURL("~who/django-42"),
	channels: []charm.Channel{charm.DevelopmentChannel},
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
		DevelopmentSeries: map[string]*charm.URL{
			"trusty": charm.MustParseURL("~who/django-42"),
			"wily":   charm.MustParseURL("~who/django-42"),
		},
	},
}, {
	about:    "development, multi series, publish development",
	url:      MustParseResolvedURL("~who/django-42"),
	channels: []charm.Channel{charm.DevelopmentChannel},
	initialEntity: &mongodoc.Entity{
		URL:             charm.MustParseURL("~who/django-42"),
		Development:     true,
		SupportedSeries: []string{"trusty", "wily"},
	},
	initialBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		DevelopmentSeries: map[string]*charm.URL{
			"precise": charm.MustParseURL("~who/django-0"),
			"trusty":  charm.MustParseURL("~who/trusty/django-0"),
		},
	},
	expectedEntity: &mongodoc.Entity{
		URL:             charm.MustParseURL("~who/django-42"),
		Development:     true,
		SupportedSeries: []string{"trusty", "wily"},
	},
	expectedBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		DevelopmentSeries: map[string]*charm.URL{
			"precise": charm.MustParseURL("~who/django-0"),
			"trusty":  charm.MustParseURL("~who/django-42"),
			"wily":    charm.MustParseURL("~who/django-42"),
		},
	},
}, {
	about:    "stable, multi series, publish development",
	url:      MustParseResolvedURL("~who/django-47"),
	channels: []charm.Channel{charm.DevelopmentChannel},
	initialEntity: &mongodoc.Entity{
		URL:             charm.MustParseURL("~who/django-47"),
		SupportedSeries: []string{"trusty", "wily", "precise"},
		Stable:          true,
	},
	initialBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		StableSeries: map[string]*charm.URL{
			"trusty": charm.MustParseURL("~who/django-47"),
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
		StableSeries: map[string]*charm.URL{
			"trusty": charm.MustParseURL("~who/django-47"),
		},
		DevelopmentSeries: map[string]*charm.URL{
			"trusty":  charm.MustParseURL("~who/django-47"),
			"wily":    charm.MustParseURL("~who/django-47"),
			"precise": charm.MustParseURL("~who/django-47"),
		},
	},
}, {
	about:    "unpublished, multi series, publish stable",
	url:      MustParseResolvedURL("~who/django-42"),
	channels: []charm.Channel{StableChannel},
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
		StableSeries: map[string]*charm.URL{
			"trusty":  charm.MustParseURL("~who/django-42"),
			"wily":    charm.MustParseURL("~who/django-42"),
			"precise": charm.MustParseURL("~who/django-42"),
		},
	},
}, {
	about:    "development, multi series, publish stable",
	url:      MustParseResolvedURL("~who/django-42"),
	channels: []charm.Channel{StableChannel},
	initialEntity: &mongodoc.Entity{
		URL:             charm.MustParseURL("~who/django-42"),
		SupportedSeries: []string{"wily"},
		Development:     true,
	},
	initialBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		DevelopmentSeries: map[string]*charm.URL{
			"trusty": charm.MustParseURL("~who/django-0"),
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
		DevelopmentSeries: map[string]*charm.URL{
			"trusty": charm.MustParseURL("~who/django-0"),
		},
		StableSeries: map[string]*charm.URL{
			"wily": charm.MustParseURL("~who/django-42"),
		},
	},
}, {
	about:    "stable, multi series, publish stable",
	url:      MustParseResolvedURL("~who/django-42"),
	channels: []charm.Channel{StableChannel},
	initialEntity: &mongodoc.Entity{
		URL:             charm.MustParseURL("~who/django-42"),
		SupportedSeries: []string{"trusty", "wily", "precise"},
		Stable:          true,
	},
	initialBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		StableSeries: map[string]*charm.URL{
			"precise": charm.MustParseURL("~who/django-1"),
			"quantal": charm.MustParseURL("~who/django-2"),
			"saucy":   charm.MustParseURL("~who/django-3"),
			"trusty":  charm.MustParseURL("~who/django-4"),
		},
	},
	expectedEntity: &mongodoc.Entity{
		URL:             charm.MustParseURL("~who/django-42"),
		SupportedSeries: []string{"trusty", "wily", "precise"},
		Stable:          true,
	},
	expectedBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		StableSeries: map[string]*charm.URL{
			"precise": charm.MustParseURL("~who/django-42"),
			"quantal": charm.MustParseURL("~who/django-2"),
			"saucy":   charm.MustParseURL("~who/django-3"),
			"trusty":  charm.MustParseURL("~who/django-42"),
			"wily":    charm.MustParseURL("~who/django-42"),
		},
	},
}, {
	about:    "bundle",
	url:      MustParseResolvedURL("~who/bundle/django-42"),
	channels: []charm.Channel{StableChannel},
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
		StableSeries: map[string]*charm.URL{
			"bundle": charm.MustParseURL("~who/bundle/django-42"),
		},
	},
}, {
	about:    "unpublished, multi series, publish multiple channels",
	url:      MustParseResolvedURL("~who/django-42"),
	channels: []charm.Channel{charm.DevelopmentChannel, StableChannel, charm.Channel("no-such")},
	initialEntity: &mongodoc.Entity{
		URL:             charm.MustParseURL("~who/django-42"),
		SupportedSeries: []string{"trusty", "wily"},
	},
	initialBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
		StableSeries: map[string]*charm.URL{
			"precise": charm.MustParseURL("~who/django-1"),
			"trusty":  charm.MustParseURL("~who/django-4"),
		},
		DevelopmentSeries: map[string]*charm.URL{
			"wily": charm.MustParseURL("~who/django-10"),
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
		DevelopmentSeries: map[string]*charm.URL{
			"trusty": charm.MustParseURL("~who/django-42"),
			"wily":   charm.MustParseURL("~who/django-42"),
		},
		StableSeries: map[string]*charm.URL{
			"precise": charm.MustParseURL("~who/django-1"),
			"trusty":  charm.MustParseURL("~who/django-42"),
			"wily":    charm.MustParseURL("~who/django-42"),
		},
	},
}, {
	about:    "not found",
	url:      MustParseResolvedURL("~who/trusty/no-such-42"),
	channels: []charm.Channel{charm.DevelopmentChannel},
	initialEntity: &mongodoc.Entity{
		URL: charm.MustParseURL("~who/trusty/django-42"),
	},
	initialBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
	},
	expectedErr: `cannot update "cs:~who/trusty/no-such-42": not found`,
}, {
	about:    "no valid channels provided",
	url:      MustParseResolvedURL("~who/trusty/django-42"),
	channels: []charm.Channel{charm.Channel("not-valid")},
	initialEntity: &mongodoc.Entity{
		URL: charm.MustParseURL("~who/trusty/django-42"),
	},
	initialBaseEntity: &mongodoc.BaseEntity{
		URL: charm.MustParseURL("~who/django"),
	},
	expectedErr: `cannot update "cs:~who/trusty/django-42": no channels provided`,
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
		err = store.Publish(test.url, test.channels...)
		if test.expectedErr != "" {
			c.Assert(err, gc.ErrorMatches, test.expectedErr)
			continue
		}
		c.Assert(err, gc.IsNil)
		entity, err := store.FindEntity(test.url, nil)
		c.Assert(err, gc.IsNil)
		c.Assert(entity, jc.DeepEquals, denormalizedEntity(test.expectedEntity))
		baseEntity, err := store.FindBaseEntity(test.url.UserOwnedURL(), nil)
		c.Assert(err, gc.IsNil)
		c.Assert(baseEntity, jc.DeepEquals, test.expectedBaseEntity)
	}
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
}, {
	about: "user owned, under development",
	entity: &mongodoc.Entity{
		URL:         charm.MustParseURL("~charmers/trusty/wordpress-42"),
		Development: true,
	},
	rurl: &router.ResolvedURL{
		URL:                 *charm.MustParseURL("~charmers/trusty/wordpress-42"),
		PromulgatedRevision: -1,
		Development:         true,
	},
}, {
	about: "promulgated, under development",
	entity: &mongodoc.Entity{
		URL:            charm.MustParseURL("~charmers/wily/wordpress-42"),
		PromulgatedURL: charm.MustParseURL("wily/wordpress-0"),
		Development:    true,
	},
	rurl: &router.ResolvedURL{
		URL:                 *charm.MustParseURL("~charmers/wily/wordpress-42"),
		PromulgatedRevision: 0,
		Development:         true,
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

func entity(url, purl string) *mongodoc.Entity {
	id := charm.MustParseURL(url)
	var pid *charm.URL
	if purl != "" {
		pid = charm.MustParseURL(purl)
	}
	e := &mongodoc.Entity{
		URL:            id,
		PromulgatedURL: pid,
		Stable:         true,
	}
	denormalizeEntity(e)
	return e
}

func baseEntity(url string, promulgated bool) *mongodoc.BaseEntity {
	id := charm.MustParseURL(url)
	return &mongodoc.BaseEntity{
		URL:         id,
		Name:        id.Name,
		User:        id.User,
		Promulgated: mongodoc.IntBool(promulgated),
	}
}

// denormalizedEntity is a convenience function that returns
// a copy of e with its denormalized fields filled out.
func denormalizedEntity(e *mongodoc.Entity) *mongodoc.Entity {
	e1 := *e
	denormalizeEntity(&e1)
	return &e1
}
