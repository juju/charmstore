// Copyright 2016 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package charmstore

import (
	"io/ioutil"
	"strconv"
	"strings"
	"time"

	jc "github.com/juju/testing/checkers"
	gc "gopkg.in/check.v1"
	"gopkg.in/errgo.v1"
	"gopkg.in/juju/charm.v6-unstable"
	"gopkg.in/juju/charmrepo.v2-unstable/csclient/params"
	"gopkg.in/mgo.v2"

	"gopkg.in/juju/charmstore.v5-unstable/internal/mongodoc"
	"gopkg.in/juju/charmstore.v5-unstable/internal/storetesting"
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
	curl := charm.MustParseURL("cs:~charmers/xenial/starsay-3")
	entity, _ := addCharm(c, store, curl)
	expected := uploadResources(c, store, entity)
	err := store.PublishResources(entity, params.StableChannel, resourceRevisions(expected))
	c.Assert(err, jc.ErrorIsNil)
	sortResources(expected)

	docs, err := store.ListResources(entity, params.StableChannel)
	c.Assert(err, jc.ErrorIsNil)

	checkResourceDocs(c, docs, expected)
}

func (s *resourceSuite) TestListResourcesCharmWithoutResources(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()
	curl := charm.MustParseURL("cs:~charmers/precise/wordpress-23")
	entity, _ := addCharm(c, store, curl)

	resources, err := store.ListResources(entity, params.StableChannel)
	c.Assert(err, jc.ErrorIsNil)

	c.Check(resources, gc.HasLen, 0)
}

func (s *resourceSuite) TestListResourcesWithNoChannel(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()
	curl := charm.MustParseURL("cs:~charmers/precise/wordpress-23")
	entity, _ := addCharm(c, store, curl)

	resources, err := store.ListResources(entity, "")
	c.Assert(err, gc.ErrorMatches, "no channel specified")
	c.Assert(resources, gc.IsNil)
}

func (s *resourceSuite) TestListResourcesBundle(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()
	curl := charm.MustParseURL("cs:~charmers/bundle/wordpress-simple-0")
	entity := addBundle(c, store, curl)

	resources, err := store.ListResources(entity, params.StableChannel)
	c.Assert(err, gc.IsNil)
	c.Assert(resources, gc.HasLen, 0)
}

func (s *resourceSuite) TestListResourcesResourceNotFound(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()
	rurl := MustParseResolvedURL("cs:~charmers/xenial/starsay-3")
	ch := storetesting.NewCharm(storetesting.MetaWithResources(nil, "resource1", "resource2"))
	err := store.AddCharmWithArchive(rurl, ch)
	c.Assert(err, jc.ErrorIsNil)
	entity, err := store.FindEntity(rurl, nil)
	c.Assert(err, jc.ErrorIsNil)
	expected := make([]*mongodoc.Resource, 2)
	expected[0] = uploadResource(c, store, entity, "resource1")
	expected[1] = &mongodoc.Resource{
		BaseURL:  mongodoc.BaseURL(&rurl.URL),
		Name:     "resource2",
		Revision: -1,
	}
	sortResources(expected)

	// A resource exists for resource1, but not resource2. Expect a
	// placeholder to be returned for resource2.
	docs, err := store.ListResources(entity, params.UnpublishedChannel)
	c.Assert(err, jc.ErrorIsNil)

	checkResourceDocs(c, docs, expected)
}

func (s *resourceSuite) TestUploadResource(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()
	curl := charm.MustParseURL("cs:~charmers/xenial/starsay-3")
	entity, _ := addCharm(c, store, curl)
	res, err := store.UploadResource(entity, "for-install", strings.NewReader("fake content"), fakeBlobHash, fakeBlobSize)
	c.Assert(err, jc.ErrorIsNil)
	c.Assert(res.Name, gc.Equals, "for-install")
	c.Assert(res.Revision, gc.Equals, 0)
	c.Assert(res.Size, gc.Equals, fakeBlobSize)
	c.Assert(res.BlobHash, gc.Equals, fakeBlobHash)
	res, err = store.UploadResource(entity, "for-install", strings.NewReader("fake content"), fakeBlobHash, fakeBlobSize)
	c.Assert(err, jc.ErrorIsNil)
	c.Assert(res.Revision, gc.Equals, 1)
}

var uploadResourceErrorTests = []struct {
	about       string
	name        string
	blob        string
	hash        string
	size        int64
	expectError string
}{{
	about:       "unrecognised name",
	name:        "bad-name",
	blob:        "fake content",
	hash:        fakeBlobHash,
	size:        fakeBlobSize,
	expectError: `charm does not have resource "bad-name"`,
}, {
	about:       "bad hash",
	name:        "for-install",
	blob:        "fake context",
	hash:        fakeBlobHash,
	size:        fakeBlobSize,
	expectError: `cannot put archive blob: hash mismatch`,
}}

func (s *resourceSuite) TestUploadResourceErrors(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()
	curl := charm.MustParseURL("cs:~charmers/xenial/starsay-3")
	entity, _ := addCharm(c, store, curl)
	for i, test := range uploadResourceErrorTests {
		c.Logf("%d. %s", i, test.about)
		_, err := store.UploadResource(entity, test.name, strings.NewReader(test.blob), test.hash, test.size)
		c.Assert(err, gc.ErrorMatches, test.expectError)
	}
}

var resolveResourceTests = []struct {
	about            string
	name             string
	revision         int
	channel          params.Channel
	expectResource   int
	expectError      string
	expectErrorCause error
}{{
	about:          "revision specified without channel",
	name:           "for-install",
	revision:       0,
	channel:        params.NoChannel,
	expectResource: 0,
}, {
	about:          "revision specified on stable channel",
	name:           "for-install",
	revision:       1,
	channel:        params.StableChannel,
	expectResource: 1,
}, {
	about:          "revision specified on development channel",
	name:           "for-install",
	revision:       2,
	channel:        params.DevelopmentChannel,
	expectResource: 2,
}, {
	about:            "revision specified that doesn't exist",
	name:             "for-install",
	revision:         3,
	channel:          params.UnpublishedChannel,
	expectError:      `cs:~charmers/xenial/starsay-3 has no "for-install/3" resource`,
	expectErrorCause: params.ErrNotFound,
}, {
	about:          "no revision specified without channel",
	name:           "for-install",
	revision:       -1,
	channel:        params.NoChannel,
	expectResource: 0,
}, {
	about:          "no revision specified on stable channel",
	name:           "for-install",
	revision:       -1,
	channel:        params.StableChannel,
	expectResource: 0,
}, {
	about:          "no revision specified on development channel",
	name:           "for-install",
	revision:       -1,
	channel:        params.DevelopmentChannel,
	expectResource: 1,
}, {
	about:          "no revision specified on unpublished channel",
	name:           "for-install",
	revision:       -1,
	channel:        params.UnpublishedChannel,
	expectResource: 2,
}, {
	about:            "no resource with name",
	name:             "for-setup",
	revision:         -1,
	channel:          params.UnpublishedChannel,
	expectError:      `cs:~charmers/xenial/starsay-3 has no "for-setup" resource`,
	expectErrorCause: params.ErrNotFound,
}}

func (s *resourceSuite) TestResolveResource(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()
	curl := charm.MustParseURL("cs:~charmers/xenial/starsay-3")
	entity, _ := addCharm(c, store, curl)

	var resources []*mongodoc.Resource

	// Upload a resource a number of times, publishing to the
	// specified channel. If the channel is params.NoChannel then the
	// resource is upload, but not published.
	for _, ch := range []params.Channel{
		params.StableChannel,
		params.DevelopmentChannel,
		params.NoChannel,
	} {
		res, err := store.UploadResource(entity, "for-install", strings.NewReader("fake content"), fakeBlobHash, fakeBlobSize)
		c.Assert(err, jc.ErrorIsNil)
		resources = append(resources, res)
		if ch == params.NoChannel {
			continue
		}
		err = store.PublishResources(entity, ch, []mongodoc.ResourceRevision{{res.Name, res.Revision}})
		c.Assert(err, jc.ErrorIsNil)
	}

	for i, test := range resolveResourceTests {
		c.Logf("%d. %s", i, test.about)
		res, err := store.ResolveResource(EntityResolvedURL(entity), test.name, test.revision, test.channel)
		if test.expectError != "" {
			c.Assert(err, gc.ErrorMatches, test.expectError)
			c.Assert(errgo.Cause(err), gc.Equals, test.expectErrorCause)
			continue
		}
		c.Assert(err, jc.ErrorIsNil)
		adjustExpectedResource(res, resources[test.expectResource])
		c.Assert(res, jc.DeepEquals, resources[test.expectResource])
	}
}

var publishResourceErrorTests = []struct {
	about       string
	channel     params.Channel
	resources   []mongodoc.ResourceRevision
	expectError string
}{{
	about:       "no channel",
	channel:     params.NoChannel,
	expectError: `missing channel`,
}, {
	about:       "unpublished channel",
	channel:     params.UnpublishedChannel,
	expectError: `cannot publish to unpublished channel`,
}, {
	about:   "unknown resource name",
	channel: params.StableChannel,
	resources: []mongodoc.ResourceRevision{{
		Name:     "for-install",
		Revision: 0,
	}, {
		Name:     "bad-name",
		Revision: 0,
	}},
	expectError: `charm does not have resource "bad-name"`,
}, {
	about:   "no such resource",
	channel: params.StableChannel,
	resources: []mongodoc.ResourceRevision{{
		Name:     "for-install",
		Revision: 0,
	}},
	expectError: `cs:~charmers/xenial/starsay-3 resource for-install revison 0 not found`,
}}

func (s *resourceSuite) TestPublishResourceErrors(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()
	curl := charm.MustParseURL("cs:~charmers/xenial/starsay-3")
	entity, _ := addCharm(c, store, curl)
	for i, test := range publishResourceErrorTests {
		c.Logf("%d. %d", i, test.about)
		err := store.PublishResources(entity, test.channel, test.resources)
		c.Assert(err, gc.ErrorMatches, test.expectError)
	}
}

func (s *resourceSuite) TestOpenResourceBlob(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()
	curl := charm.MustParseURL("cs:~charmers/xenial/starsay-3")
	entity, _ := addCharm(c, store, curl)
	res, err := store.UploadResource(entity, "for-install", strings.NewReader("fake content"), fakeBlobHash, fakeBlobSize)
	c.Assert(err, jc.ErrorIsNil)
	blob, err := store.OpenResourceBlob(res)
	c.Assert(err, jc.ErrorIsNil)
	defer blob.Close()
	c.Assert(blob.Size, gc.Equals, fakeBlobSize)
	c.Assert(blob.Hash, gc.Equals, fakeBlobHash)
	data, err := ioutil.ReadAll(blob)
	c.Assert(err, jc.ErrorIsNil)
	c.Assert(string(data), gc.Equals, "fake content")

	// Change the blob name so that it's invalid
	// so that we can check what happens then.
	res.BlobName = res.BlobName[1:]
	_, err = store.OpenResourceBlob(res)
	c.Assert(err, gc.ErrorMatches, `cannot open archive data for cs:~charmers/starsay resource "for-install"/0: .*`)
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
