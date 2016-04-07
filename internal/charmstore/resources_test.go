// Copyright 2016 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package charmstore

import (
	"strconv"
	"strings"
	"time"

	jc "github.com/juju/testing/checkers"
	gc "gopkg.in/check.v1"
	"gopkg.in/juju/charm.v6-unstable"
	"gopkg.in/juju/charmrepo.v2-unstable/csclient/params"
	"gopkg.in/mgo.v2"

	"gopkg.in/juju/charmstore.v5-unstable/internal/mongodoc"
)

type resourceSuite struct {
	commonSuite
}

var _ = gc.Suite(&resourceSuite{})

func (s *resourceSuite) TestInsert(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()
	r := &mongodoc.Resource{
		BaseURL:    charm.MustParseURL("~bob/wordpress"),
		Name:       "resource-1",
		Revision:   0,
		BlobHash:   "123456",
		Size:       1,
		BlobName:   "res1",
		UploadTime: time.Now().UTC(),
	}

	// First insert works correctly.
	err := store.DB.Resources().Insert(r)
	c.Assert(err, jc.ErrorIsNil)

	// Attempting to insert the same revision fails.
	r.BlobHash = "78910"
	err = store.DB.Resources().Insert(r)
	c.Assert(mgo.IsDup(err), gc.Equals, true)

	// Inserting a different revision succeeds.
	r.Revision = 1
	err = store.DB.Resources().Insert(r)
	c.Assert(err, jc.ErrorIsNil)
}

var newResourceQueryTests = []struct {
	about           string
	url             *charm.URL
	name            string
	revision        int
	expectResources []*mongodoc.Resource
}{{
	about:    "without revision",
	url:      charm.MustParseURL("~bob/wordpress"),
	name:     "res",
	revision: -1,
	expectResources: []*mongodoc.Resource{{
		BaseURL:  charm.MustParseURL("~bob/wordpress"),
		Name:     "res",
		Revision: 0,
	}, {
		BaseURL:  charm.MustParseURL("~bob/wordpress"),
		Name:     "res",
		Revision: 1,
	}},
}, {
	about:    "with revision",
	url:      charm.MustParseURL("~bob/wordpress"),
	name:     "res",
	revision: 1,
	expectResources: []*mongodoc.Resource{{
		BaseURL:  charm.MustParseURL("~bob/wordpress"),
		Name:     "res",
		Revision: 1,
	}},
}}

func (s *resourceSuite) TestNewResourceQuery(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()
	for _, r := range []string{
		"~bob/wordpress|res|0",
		"~bob/wordpress|res|1",
		"~bob/wordpress|res2|0",
		"~bob/wordpress|res2|1",
		"~bob/mysql|res|0",
		"~alice/wordpress|res|0",
	} {
		parts := strings.SplitN(r, "|", 3)
		rev, err := strconv.Atoi(parts[2])
		c.Assert(err, jc.ErrorIsNil)
		err = store.DB.Resources().Insert(&mongodoc.Resource{
			BaseURL:  charm.MustParseURL(parts[0]),
			Name:     parts[1],
			Revision: rev,
		})
		c.Assert(err, jc.ErrorIsNil)
	}
	for i, test := range newResourceQueryTests {
		c.Logf("%d. %s", i, test.about)
		q := newResourceQuery(test.url, test.name, test.revision)
		var results []*mongodoc.Resource
		err := store.DB.Resources().Find(q).All(&results)
		c.Assert(err, jc.ErrorIsNil)
		sortResources(test.expectResources)
		sortResources(results)
		c.Assert(results, jc.DeepEquals, test.expectResources)
	}
}

func (s *resourceSuite) TestListResourcesCharmWithResources(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()
	channel := params.StableChannel
	curl := charm.MustParseURL("cs:~charmers/xenial/starsay-3")
	entity, _ := addCharm(c, store, curl)
	expected := uploadResources(c, store, entity)
	err := store.publishResources(entity, channel, resourceRevisions(expected))
	c.Assert(err, jc.ErrorIsNil)
	sortResources(expected)

	docs, err := store.ListResources(entity, channel)
	c.Assert(err, jc.ErrorIsNil)

	checkResourceDocs(c, docs, expected)
}

func (s *resourceSuite) TestListResourcesCharmWithoutResources(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()
	channel := params.StableChannel
	curl := charm.MustParseURL("cs:~charmers/precise/wordpress-23")
	entity, _ := addCharm(c, store, curl)

	resources, err := store.ListResources(entity, channel)
	c.Assert(err, jc.ErrorIsNil)

	c.Check(resources, gc.HasLen, 0)
}

func (s *resourceSuite) TestListResourcesBundle(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()
	channel := params.StableChannel
	curl := charm.MustParseURL("cs:~charmers/bundle/wordpress-simple-0")
	entity := addBundle(c, store, curl)

	resources, err := store.ListResources(entity, channel)
	c.Assert(err, gc.IsNil)
	c.Assert(resources, gc.HasLen, 0)
}

func (s *resourceSuite) TestListResourcesResourceNotFound(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()
	channel := params.StableChannel
	curl := charm.MustParseURL("cs:~charmers/xenial/starsay-3")
	entity, _ := addCharm(c, store, curl)
	expected := uploadResources(c, store, entity)
	sortResources(expected)
	err := store.publishResources(entity, channel, resourceRevisions(expected[1:]))
	c.Assert(err, jc.ErrorIsNil)

	docs, err := store.ListResources(entity, channel)
	c.Assert(err, jc.ErrorIsNil)

	checkResourceDocs(c, docs, expected[1:])
}

func uploadResources(c *gc.C, store *Store, entity *mongodoc.Entity) []*mongodoc.Resource {
	var resources []*mongodoc.Resource
	for _, m := range entity.CharmMeta.Resources {
		resources = append(resources, uploadResource(c, store, entity, m.Name))
	}
	return resources
}

func uploadResource(c *gc.C, store *Store, entity *mongodoc.Entity, name string) *mongodoc.Resource {
	r := strings.NewReader("fake content")
	res, err := store.UploadResource(entity, name, r, fakeBlobHash, fakeBlobSize)
	c.Assert(err, jc.ErrorIsNil)
	return res
}

func resourceRevisions(resources []*mongodoc.Resource) []mongodoc.ResourceRevision {
	revisions := make([]mongodoc.ResourceRevision, len(resources))
	for i := range resources {
		revisions[i].Name = resources[i].Name
		revisions[i].Revision = resources[i].Revision
	}
	return revisions
}

func checkResourceDocs(c *gc.C, docs, expected []*mongodoc.Resource) {
	sortResources(docs)
	for i, doc := range docs {
		adjustExpectedResource(doc, expected[i])
	}

	c.Check(docs, jc.DeepEquals, expected)
}

func adjustExpectedResource(doc, expected *mongodoc.Resource) {
	expected.BlobName = doc.BlobName
	expected.UploadTime = doc.UploadTime
}
