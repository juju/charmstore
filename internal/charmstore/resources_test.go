// Copyright 2016 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package charmstore

import (
	"fmt"
	"io/ioutil"
	"strconv"
	"strings"
	"time"

	jc "github.com/juju/testing/checkers"
	gc "gopkg.in/check.v1"
	"gopkg.in/errgo.v1"
	"gopkg.in/juju/charm.v6"
	"gopkg.in/juju/charm.v6/resource"
	"gopkg.in/juju/charmrepo.v3/csclient/params"
	"gopkg.in/mgo.v2"

	"gopkg.in/juju/charmstore.v5/internal/blobstore"
	"gopkg.in/juju/charmstore.v5/internal/mongodoc"
	"gopkg.in/juju/charmstore.v5/internal/router"
	"gopkg.in/juju/charmstore.v5/internal/storetesting"
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
		UploadTime: time.Now().UTC(),
	}

	// First insert works correctly.
	err := store.DB.Resources().Insert(r)
	c.Assert(err, gc.Equals, nil)

	// Attempting to insert the same revision fails.
	r.BlobHash = "78910"
	err = store.DB.Resources().Insert(r)
	c.Assert(mgo.IsDup(err), gc.Equals, true)

	// Inserting a different revision succeeds.
	r.Revision = 1
	err = store.DB.Resources().Insert(r)
	c.Assert(err, gc.Equals, nil)
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
		c.Assert(err, gc.Equals, nil)
		err = store.DB.Resources().Insert(&mongodoc.Resource{
			BaseURL:  charm.MustParseURL(parts[0]),
			Name:     parts[1],
			Revision: rev,
		})
		c.Assert(err, gc.Equals, nil)
	}
	for i, test := range newResourceQueryTests {
		c.Logf("%d. %s", i, test.about)
		q := newResourceQuery(test.url, test.name, test.revision)
		var results []*mongodoc.Resource
		err := store.DB.Resources().Find(q).All(&results)
		c.Assert(err, gc.Equals, nil)
		sortResources(test.expectResources)
		sortResources(results)
		c.Assert(results, jc.DeepEquals, test.expectResources)
	}
}

func (s *resourceSuite) TestListResourcesCharmWithResources(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()

	id := MustParseResolvedURL("cs:~charmers/precise/wordpress-3")
	meta := storetesting.MetaWithResources(nil, "resource1", "resource2")
	err := store.AddCharmWithArchive(id, storetesting.NewCharm(meta))
	c.Assert(err, gc.Equals, nil)
	uploadResources(c, store, id, "")

	err = store.Publish(id, map[string]int{
		"resource1": 0,
		"resource2": 0,
	}, params.StableChannel)
	c.Assert(err, gc.Equals, nil)

	docs, err := store.ListResources(id, params.StableChannel)
	c.Assert(err, gc.Equals, nil)

	checkResourceDocs(c, store, id, []string{"resource1/0", "resource2/0"}, docs)
}

func (s *resourceSuite) TestListResourcesCharmWithoutResources(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()

	id := MustParseResolvedURL("cs:~charmers/precise/wordpress-3")
	err := store.AddCharmWithArchive(id, storetesting.NewCharm(nil))
	c.Assert(err, gc.Equals, nil)

	resources, err := store.ListResources(id, params.StableChannel)
	c.Assert(err, gc.Equals, nil)

	c.Check(resources, gc.HasLen, 0)
}

func (s *resourceSuite) TestListResourcesWithNoChannel(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()

	id := MustParseResolvedURL("cs:~charmers/precise/wordpress-3")
	err := store.AddCharmWithArchive(id, storetesting.NewCharm(nil))
	c.Assert(err, gc.Equals, nil)

	resources, err := store.ListResources(id, "")
	c.Assert(err, gc.ErrorMatches, "no channel specified")
	c.Assert(resources, gc.IsNil)
}

func (s *resourceSuite) TestListResourcesBundle(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()

	id := MustParseResolvedURL("cs:~charmers/bundle/wordpress-simple-0")
	b := storetesting.NewBundle(&charm.BundleData{
		Applications: map[string]*charm.ApplicationSpec{
			"wordpress": {
				Charm: "cs:utopic/wordpress-0",
			},
		},
	})
	s.addRequiredCharms(c, b)
	err := store.AddBundleWithArchive(id, b)
	c.Assert(err, gc.Equals, nil)

	resources, err := store.ListResources(id, params.StableChannel)
	c.Assert(err, gc.Equals, nil)
	c.Assert(resources, gc.HasLen, 0)
}

func (s *resourceSuite) TestListResourcesResourceNotFound(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()
	id := MustParseResolvedURL("cs:~charmers/precise/wordpress-3")
	ch := storetesting.NewCharm(storetesting.MetaWithResources(nil, "resource1", "resource2"))
	err := store.AddCharmWithArchive(id, ch)
	c.Assert(err, gc.Equals, nil)
	uploadResource(c, store, id, "resource1", "something")

	// A resource exists for resource1, but not resource2. Expect a
	// placeholder to be returned for resource2.
	docs, err := store.ListResources(id, params.UnpublishedChannel)
	c.Assert(err, gc.Equals, nil)

	checkResourceDocs(c, store, id, []string{"resource1/0", "resource2/-1"}, docs)
}

func (s *resourceSuite) TestListResourcesCharmWithDockerResources(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()

	id := MustParseResolvedURL("cs:~charmers/caas-0")
	meta := storetesting.MetaWithDockerResources(nil, "resource1", "resource2")
	meta = storetesting.MetaWithSupportedSeries(meta, "kubernetes")
	err := store.AddCharmWithArchive(id, storetesting.NewCharm(meta))
	c.Assert(err, gc.Equals, nil)
	uploadResources(c, store, id, "")

	err = store.Publish(id, map[string]int{
		"resource1": 0,
		"resource2": 0,
	}, params.StableChannel)
	c.Assert(err, gc.Equals, nil)

	docs, err := store.ListResources(id, params.StableChannel)
	c.Assert(err, gc.Equals, nil)

	checkResourceDocs(c, store, id, []string{"resource1/0", "resource2/0"}, docs)
}

func (s *resourceSuite) TestListResourcesCharmWithDockerResourcesUnpublished(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()

	id := MustParseResolvedURL("cs:~charmers/caas-0")
	meta := storetesting.MetaWithDockerResources(nil, "resource1", "resource2")
	meta = storetesting.MetaWithSupportedSeries(meta, "kubernetes")
	err := store.AddCharmWithArchive(id, storetesting.NewCharm(meta))
	c.Assert(err, gc.Equals, nil)
	uploadResources(c, store, id, "")

	docs, err := store.ListResources(id, params.UnpublishedChannel)
	c.Assert(err, gc.Equals, nil)

	checkResourceDocs(c, store, id, []string{"resource1/0", "resource2/0"}, docs)
}

func (s *resourceSuite) TestUploadResource(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()

	id := MustParseResolvedURL("cs:~charmers/precise/wordpress-3")
	meta := storetesting.MetaWithResources(nil, "someResource")
	err := store.AddCharmWithArchive(id, storetesting.NewCharm(meta))
	c.Assert(err, gc.Equals, nil)

	now := time.Now()
	blob := "content 1"
	res, err := store.UploadResource(id, "someResource", strings.NewReader(blob), hashOfString(blob), int64(len(blob)))
	c.Assert(err, gc.Equals, nil)
	if res.UploadTime.Before(now) {
		c.Fatalf("upload time earlier than expected; want > %v; got %v", now, res.UploadTime)
	}
	checkResourceDocs(c, store, id, []string{"someResource/0"}, []*mongodoc.Resource{res})

	blob = "content 2"
	res, err = store.UploadResource(id, "someResource", strings.NewReader(blob), hashOfString(blob), int64(len(blob)))
	c.Assert(err, gc.Equals, nil)
	checkResourceDocs(c, store, id, []string{"someResource/1"}, []*mongodoc.Resource{res})
}

func (s *resourceSuite) TestAddResourceWithUploadId(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()

	id := MustParseResolvedURL("cs:~charmers/precise/wordpress-3")
	meta := storetesting.MetaWithResources(nil, "someResource")
	err := store.AddCharmWithArchive(id, storetesting.NewCharm(meta))
	c.Assert(err, gc.Equals, nil)

	contents := []string{
		"123456789 123456789 ",
		"abcdefghijklmnopqrstuvwyz",
	}
	uid := putMultipart(c, store.BlobStore, time.Time{}, contents...)

	res, err := store.AddResourceWithUploadId(id, "someResource", uid)
	c.Assert(err, gc.Equals, nil)

	// Check that the upload document has been removed.
	_, err = store.BlobStore.UploadInfo(uid)
	c.Assert(errgo.Cause(err), gc.Equals, blobstore.ErrNotFound)

	checkResourceDocs(c, store, id, []string{"someResource/0"}, []*mongodoc.Resource{res})
	allContents := strings.Join(contents, "")
	blob, err := store.OpenResourceBlob(res)
	c.Assert(err, gc.Equals, nil)
	defer blob.Close()
	c.Assert(blob.Size, gc.Equals, int64(len(allContents)))
	c.Assert(blob.Hash, gc.Equals, hashOfString(allContents))
	data, err := ioutil.ReadAll(blob)
	c.Assert(err, gc.Equals, nil)
	c.Assert(string(data), gc.Equals, allContents)
}

func (s *resourceSuite) TestAddResourceWithSharedUploadId(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()

	id := MustParseResolvedURL("cs:~charmers/precise/wordpress-3")
	meta := storetesting.MetaWithResources(nil, "someResource")
	err := store.AddCharmWithArchive(id, storetesting.NewCharm(meta))
	c.Assert(err, gc.Equals, nil)

	contents := []string{
		"123456789 123456789 ",
		"abcdefghijklmnopqrstuvwyz",
	}
	uid := putMultipart(c, store.BlobStore, time.Time{}, contents...)

	err = store.BlobStore.SetOwner(uid, "test", time.Now().Add(time.Minute))
	c.Assert(err, gc.Equals, nil)

	// We get an error but the upload should not be removed.
	_, err = store.AddResourceWithUploadId(id, "someResource", uid)
	c.Assert(err, gc.ErrorMatches, `cannot set owner of upload: upload already used by something else`)

	// Check that the blob is still around.
	info, err := store.BlobStore.UploadInfo(uid)
	c.Assert(err, gc.Equals, nil)

	idx, ok := info.Index()
	c.Assert(ok, gc.Equals, true)
	r, _, err := store.BlobStore.Open(uid, idx)
	c.Assert(err, gc.Equals, nil)
	defer r.Close()
	data, err := ioutil.ReadAll(r)
	c.Assert(err, gc.Equals, nil)
	c.Assert(string(data), gc.Equals, strings.Join(contents, ""))
}

func putMultipart(c *gc.C, bs *blobstore.Store, expires time.Time, contents ...string) string {
	if expires.IsZero() {
		expires = time.Now().Add(time.Minute)
	}
	id, err := bs.NewUpload(expires)
	c.Assert(err, gc.Equals, nil)

	parts := make([]blobstore.Part, len(contents))
	pos := int64(0)
	for i, content := range contents {
		hash := hashOfString(content)
		err = bs.PutPart(id, i, strings.NewReader(content), int64(len(content)), pos, hash)
		c.Assert(err, gc.Equals, nil)
		parts[i].Hash = hash
		pos += int64(len(content))
	}
	_, _, err = bs.FinishUpload(id, parts)
	c.Assert(err, gc.Equals, nil)
	return id
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
	blob:        fakeContent,
	hash:        fakeBlobHash,
	size:        fakeBlobSize,
	expectError: `charm does not have resource "bad-name"`,
}, {
	about:       "bad hash",
	name:        "someResource",
	blob:        "fake context",
	hash:        fakeBlobHash,
	size:        fakeBlobSize,
	expectError: `cannot put archive blob: hash mismatch`,
}}

func (s *resourceSuite) TestUploadResourceErrors(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()

	id := MustParseResolvedURL("cs:~charmers/precise/wordpress-3")
	meta := storetesting.MetaWithResources(nil, "someResource")
	err := store.AddCharmWithArchive(id, storetesting.NewCharm(meta))
	c.Assert(err, gc.Equals, nil)

	for i, test := range uploadResourceErrorTests {
		c.Logf("%d. %s", i, test.about)
		_, err = store.UploadResource(id, test.name, strings.NewReader(test.blob), test.hash, test.size)
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
	name:           "someResource",
	revision:       0,
	channel:        params.NoChannel,
	expectResource: 0,
}, {
	about:          "revision specified on stable channel",
	name:           "someResource",
	revision:       1,
	channel:        params.StableChannel,
	expectResource: 1,
}, {
	about:          "revision specified on edge channel",
	name:           "someResource",
	revision:       2,
	channel:        params.EdgeChannel,
	expectResource: 2,
}, {
	about:            "revision specified that doesn't exist",
	name:             "someResource",
	revision:         3,
	channel:          params.UnpublishedChannel,
	expectError:      `cs:~charmers/precise/wordpress-3 has no "someResource/3" resource`,
	expectErrorCause: params.ErrNotFound,
}, {
	about:          "no revision specified without channel",
	name:           "someResource",
	revision:       -1,
	channel:        params.NoChannel,
	expectResource: 0,
}, {
	about:          "no revision specified on stable channel",
	name:           "someResource",
	revision:       -1,
	channel:        params.StableChannel,
	expectResource: 0,
}, {
	about:          "no revision specified on edge channel",
	name:           "someResource",
	revision:       -1,
	channel:        params.EdgeChannel,
	expectResource: 1,
}, {
	about:          "no revision specified on unpublished channel",
	name:           "someResource",
	revision:       -1,
	channel:        params.UnpublishedChannel,
	expectResource: 2,
}, {
	about:            "no resource with name",
	name:             "otherResource",
	revision:         -1,
	channel:          params.UnpublishedChannel,
	expectError:      `cs:~charmers/precise/wordpress-3 has no "otherResource" resource`,
	expectErrorCause: params.ErrNotFound,
}}

func (s *resourceSuite) TestResolveResource(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()

	id := MustParseResolvedURL("cs:~charmers/precise/wordpress-3")
	meta := storetesting.MetaWithResources(nil, "someResource")
	err := store.AddCharmWithArchive(id, storetesting.NewCharm(meta))
	c.Assert(err, gc.Equals, nil)

	// Upload three version of the resource.
	for i := 0; i < 3; i++ {
		content := fmt.Sprintf("content%d", i)
		_, err := store.UploadResource(id, "someResource", strings.NewReader(content), hashOfString(content), int64(len(content)))
		c.Assert(err, gc.Equals, nil)
	}
	// Publish the charm to different channels with the different resources.
	err = store.Publish(id, map[string]int{
		"someResource": 0,
	}, params.StableChannel)
	c.Assert(err, gc.Equals, nil)

	err = store.Publish(id, map[string]int{
		"someResource": 1,
	}, params.EdgeChannel)
	c.Assert(err, gc.Equals, nil)

	for i, test := range resolveResourceTests {
		c.Logf("%d. %s", i, test.about)
		res, err := store.ResolveResource(id, test.name, test.revision, test.channel)
		if test.expectError != "" {
			c.Assert(err, gc.ErrorMatches, test.expectError)
			c.Assert(errgo.Cause(err), gc.Equals, test.expectErrorCause)
			continue
		}
		c.Assert(err, gc.Equals, nil)
		checkResourceDocs(c, store, id, []string{fmt.Sprintf("someResource/%d", test.expectResource)}, []*mongodoc.Resource{res})
	}
}

func (s *resourceSuite) TestPublishWithResourceNotInMetadata(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()

	id := MustParseResolvedURL("cs:~charmers/precise/wordpress-3")
	err := store.AddCharmWithArchive(id, storetesting.NewCharm(nil))
	c.Assert(err, gc.Equals, nil)

	err = store.Publish(id, map[string]int{
		"resource1": 0,
	}, params.StableChannel)
	c.Assert(err, gc.ErrorMatches, `charm does not have resource "resource1"`)
	c.Assert(errgo.Cause(err), gc.Equals, ErrPublishResourceMismatch)
}

func (s *resourceSuite) TestPublishWithResourceNotFound(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()

	id := MustParseResolvedURL("cs:~charmers/precise/wordpress-3")
	meta := storetesting.MetaWithResources(nil, "resource1")
	err := store.AddCharmWithArchive(id, storetesting.NewCharm(meta))
	c.Assert(err, gc.Equals, nil)

	err = store.Publish(id, map[string]int{
		"resource1": 0,
	}, params.StableChannel)
	c.Assert(err, gc.ErrorMatches, `cs:~charmers/precise/wordpress-3 resource "resource1/0" not found`)
	c.Assert(errgo.Cause(err), gc.Equals, ErrPublishResourceMismatch)
}

func (s *resourceSuite) TestPublishWithoutAllRequiredResources(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()

	id := MustParseResolvedURL("cs:~charmers/precise/wordpress-3")
	meta := storetesting.MetaWithResources(nil, "resource1", "resource2", "resource3")
	err := store.AddCharmWithArchive(id, storetesting.NewCharm(meta))
	c.Assert(err, gc.Equals, nil)

	uploadResource(c, store, id, "resource2", "content")

	err = store.Publish(id, map[string]int{
		"resource2": 0,
	}, params.StableChannel)
	c.Assert(err, gc.ErrorMatches, `resources are missing from publish request: resource1, resource3`)
	c.Assert(errgo.Cause(err), gc.Equals, ErrPublishResourceMismatch)
}

func (s *resourceSuite) TestOpenResourceBlob(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()

	meta := storetesting.MetaWithResources(nil, "someResource")
	id := MustParseResolvedURL("cs:~charmers/precise/wordpress-3")
	err := store.AddCharmWithArchive(id, storetesting.NewCharm(meta))
	c.Assert(err, gc.Equals, nil)

	content := "some content"
	uploadResource(c, store, id, "someResource", content)

	res, err := store.ResolveResource(id, "someResource", -1, params.UnpublishedChannel)
	c.Assert(err, gc.Equals, nil)

	blob, err := store.OpenResourceBlob(res)
	c.Assert(err, gc.Equals, nil)
	defer blob.Close()
	c.Assert(blob.Size, gc.Equals, int64(len(content)))
	c.Assert(blob.Hash, gc.Equals, hashOfString(content))
	data, err := ioutil.ReadAll(blob)
	c.Assert(err, gc.Equals, nil)
	c.Assert(string(data), gc.Equals, content)

	// Change the blob hash so that it's invalid so that we can
	// check what happens then.
	res.BlobHash = res.BlobHash[1:]
	_, err = store.OpenResourceBlob(res)
	c.Assert(err, gc.ErrorMatches, `cannot open archive data for cs:~charmers/wordpress resource "someResource/0": blob not found`)
}

func (s *resourceSuite) TestAddDockerResource(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()

	meta := storetesting.MetaWithDockerResources(nil, "resource1")
	meta = storetesting.MetaWithSupportedSeries(meta, "kubernetes")
	id := MustParseResolvedURL("cs:~charmers/docker-registry-0")
	err := store.AddCharmWithArchive(id, storetesting.NewCharm(meta))
	c.Assert(err, gc.Equals, nil)

	res, err := store.AddDockerResource(id, "resource1", "registry.example.com/library/image", "crc32:363a3020")
	c.Assert(err, gc.Equals, nil)
	checkResourceDocs(c, store, id, []string{"resource1/0"}, []*mongodoc.Resource{res})
}

func (s *resourceSuite) TestAddDockerResourceNoSuchCharm(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()

	id := MustParseResolvedURL("cs:~charmers/docker-registry-0")

	_, err := store.AddDockerResource(id, "resource1", "registry.example.com/library/image", "crc32:363a3020")
	c.Assert(err, gc.ErrorMatches, `entity not found`)
	c.Assert(errgo.Cause(err), gc.Equals, params.ErrNotFound)
}

func (s *resourceSuite) TestAddDockerResourceNotKubernetesCharm(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()

	meta := storetesting.MetaWithDockerResources(nil, "resource1")
	meta = storetesting.MetaWithSupportedSeries(meta, "bionic")
	id := MustParseResolvedURL("cs:~charmers/docker-registry-0")
	err := store.AddCharmWithArchive(id, storetesting.NewCharm(meta))
	c.Assert(err, gc.Equals, nil)

	_, err = store.AddDockerResource(id, "resource1", "registry.example.com/library/image", "crc32:363a3020")
	c.Assert(err, gc.ErrorMatches, `entity is not a kubernetes charm`)
}

func (s *resourceSuite) TestAddDockerResourceNoSuchResource(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()

	meta := storetesting.MetaWithDockerResources(nil, "resource1")
	meta = storetesting.MetaWithSupportedSeries(meta, "kubernetes")
	id := MustParseResolvedURL("cs:~charmers/docker-registry-0")
	err := store.AddCharmWithArchive(id, storetesting.NewCharm(meta))
	c.Assert(err, gc.Equals, nil)

	_, err = store.AddDockerResource(id, "resource2", "registry.example.com/library/image", "crc32:363a3020")
	c.Assert(err, gc.ErrorMatches, `"cs:~charmers/docker-registry-0" does not have image resource "resource2"`)
}

// uploadResources uploads all the resources required by the given
// entity. File resources contain blob content that's the resource name
// followed by the given content suffix. Docker resources have a digest
// generated from the resource name followed by the given content suffix.
func uploadResources(c *gc.C, store *Store, id *router.ResolvedURL, contentSuffix string) {
	entity, err := store.FindEntity(id, nil)
	c.Assert(err, gc.Equals, nil)
	for name, res := range entity.CharmMeta.Resources {
		c.Logf("uploading resource %v", name)
		content := name + contentSuffix
		hash := hashOfString(content)
		r := strings.NewReader(content)
		switch res.Type {
		case resource.TypeFile:
			_, err := store.UploadResource(id, name, r, hash, int64(len(content)))
			c.Assert(err, gc.Equals, nil)
		case resource.TypeContainerImage:
			_, err := store.AddDockerResource(id, name, "", "test-hash:"+hash)
			c.Assert(err, gc.Equals, nil)
		}
	}
}

func uploadResource(c *gc.C, store *Store, id *router.ResolvedURL, name string, blob string) {
	_, err := store.UploadResource(id, name, strings.NewReader(blob), hashOfString(blob), int64(len(blob)))
	c.Assert(err, gc.Equals, nil)
}

func resourceRevisions(resources []*mongodoc.Resource) map[string]int {
	revisions := make(map[string]int)
	for _, r := range resources {
		revisions[r.Name] = r.Revision
	}
	return revisions
}

// checkResourceDocs checks that the resource documents in docs match
// the named resources held by the charm with the given base URL.
// The resource names are parsed with parseResourceId.
//
// If a resource revision is specified as -1, it is expected to
// be a placeholder in docs.
func checkResourceDocs(c *gc.C, store *Store, id *router.ResolvedURL, expectResources []string, docs []*mongodoc.Resource) {
	c.Assert(expectResources, gc.HasLen, len(docs))
	for i, ridStr := range expectResources {
		doc := docs[i]
		rid := parseResourceId(ridStr)
		if rid.Revision == -1 {
			// No revision implies we want a placeholder doc.
			c.Assert(doc, jc.DeepEquals, &mongodoc.Resource{
				BaseURL:  mongodoc.BaseURL(&id.URL),
				Name:     rid.Name,
				Revision: -1,
			}, gc.Commentf("resource %v/%d", rid.Name, rid.Revision))
			continue
		}
		expectDoc, err := store.ResolveResource(id, rid.Name, rid.Revision, params.UnpublishedChannel)
		c.Assert(err, gc.Equals, nil, gc.Commentf("resource %v/%d", rid.Name, rid.Revision))

		// Mongo's time stamps are only accurate to a millisecond.
		// If we're checking against a document that's been created
		// locally, rather than pulled from the database, there might be
		// up to a millisecond of discrepancy.
		if doc.UploadTime.Before(expectDoc.UploadTime.Add(-time.Millisecond)) ||
			doc.UploadTime.After(expectDoc.UploadTime.Add(time.Millisecond)) {
			c.Fatalf("upload time mismatch; got %v want %v", doc.UploadTime, expectDoc.UploadTime)
		}
		doc.UploadTime = expectDoc.UploadTime
		c.Assert(doc, jc.DeepEquals, expectDoc, gc.Commentf("resource %v/%d", rid.Name, rid.Revision))
	}
}

func adjustExpectedResource(doc, expected *mongodoc.Resource) {
	expected.UploadTime = doc.UploadTime
}

func parseResourceId(s string) mongodoc.ResourceRevision {
	i := strings.Index(s, "/")
	if i == -1 {
		panic(fmt.Sprintf("no revision in %q", s))
	}
	rev, err := strconv.Atoi(s[i+1:])
	if err != nil {
		panic(fmt.Sprintf("invalid resource revision in %q", s))
	}
	return mongodoc.ResourceRevision{
		Name:     s[0:i],
		Revision: rev,
	}
}
