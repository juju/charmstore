// Copyright 2016 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package charmstore

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	jc "github.com/juju/testing/checkers"
	gc "gopkg.in/check.v1"
	"gopkg.in/juju/charm.v6-unstable"
	"gopkg.in/juju/charm.v6-unstable/resource"
	"gopkg.in/juju/charmrepo.v2-unstable/csclient/params"
	"gopkg.in/mgo.v2/bson"

	"gopkg.in/juju/charmstore.v5-unstable/internal/mongodoc"
)

type ResourcesSuite struct {
	commonSuite
}

var _ = gc.Suite(&ResourcesSuite{})

func (s *ResourcesSuite) TestListResourcesCharmWithResources(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()
	channel := params.StableChannel
	curl := charm.MustParseURL("cs:~charmers/xenial/starsay-3")
	entity, ch := addCharm(c, store, curl)
	expected := addResources(c, store, curl, channel, entity, ch)
	mongodoc.SortResources(expected)

	docs, err := store.ListResources(entity, channel)
	c.Assert(err, jc.ErrorIsNil)

	checkResourceDocs(c, docs, expected)
}

func (s *ResourcesSuite) TestListResourcesCharmWithoutResources(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()
	channel := params.StableChannel
	curl := charm.MustParseURL("cs:~charmers/precise/wordpress-23")
	entity, _ := addCharm(c, store, curl)

	resources, err := store.ListResources(entity, channel)
	c.Assert(err, jc.ErrorIsNil)

	c.Check(resources, gc.HasLen, 0)
}

func (s *ResourcesSuite) TestListResourcesBundle(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()
	channel := params.StableChannel
	curl := charm.MustParseURL("cs:~charmers/bundle/wordpress-simple-0")
	entity := addBundle(c, store, curl)

	_, err := store.ListResources(entity, channel)

	c.Check(err, gc.ErrorMatches, `bundles do not have resources`)
}

func (s *ResourcesSuite) TestListResourcesResourceNotFound(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()
	channel := params.StableChannel
	curl := charm.MustParseURL("cs:~charmers/xenial/starsay-3")
	entity, ch := addCharm(c, store, curl)
	expected := extractResources(c, curl, ch)
	mongodoc.SortResources(expected)
	expected[0] = &mongodoc.Resource{
		CharmURL: expected[0].CharmURL,
		Name:     expected[0].Name,
	}
	expected[1].Revision = addResource(c, store, entity, channel, expected[1], nil)
	expected[2].Revision = addResource(c, store, entity, channel, expected[2], nil)

	docs, err := store.ListResources(entity, channel)
	c.Assert(err, jc.ErrorIsNil)

	checkResourceDocs(c, docs, expected)
}

func (s *ResourcesSuite) TestOpenResource(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()
	curl := charm.MustParseURL("cs:~charmers/xenial/starsay-3")
	resolvedURL := MustParseResolvedURL(curl.String())
	entity, ch := addCharm(c, store, curl)
	allExpected := addResources(c, store, curl, params.UnpublishedChannel, entity, ch)
	mongodoc.SortResources(allExpected)
	expected := allExpected[1]
	meta := ch.Meta().Resources["for-store"]
	expectedData, err := ioutil.ReadFile(filepath.Join(ch.Path, meta.Path))

	doc, reader, err := store.OpenResource(resolvedURL, meta.Name, 1)
	c.Assert(err, jc.ErrorIsNil)
	defer reader.Close()

	adjustExpectedResource(doc, expected)
	c.Check(doc, jc.DeepEquals, expected)
	data, err := ioutil.ReadAll(reader)
	c.Assert(err, jc.ErrorIsNil)
	c.Check(data, jc.DeepEquals, expectedData)
}

func (s *ResourcesSuite) TestOpenLatestResource(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()
	channel := params.StableChannel
	curl := charm.MustParseURL("cs:~charmers/xenial/starsay-3")
	resolvedURL := MustParseResolvedURL(curl.String())
	entity, ch := addCharm(c, store, curl)
	allExpected := addResources(c, store, curl, channel, entity, ch)
	mongodoc.SortResources(allExpected)
	expected := allExpected[1]
	meta := ch.Meta().Resources["for-store"]
	expectedData, err := ioutil.ReadFile(filepath.Join(ch.Path, meta.Path))

	doc, reader, err := store.OpenLatestResource(resolvedURL, channel, meta.Name)
	c.Assert(err, jc.ErrorIsNil)
	defer reader.Close()

	adjustExpectedResource(doc, expected)
	c.Check(doc, jc.DeepEquals, expected)
	data, err := ioutil.ReadAll(reader)
	c.Assert(err, jc.ErrorIsNil)
	c.Check(data, jc.DeepEquals, expectedData)
}

func addResources(c *gc.C, store *Store, curl *charm.URL, channel params.Channel, entity *mongodoc.Entity, ch *charm.CharmDir) []*mongodoc.Resource {
	docs := extractResources(c, curl, ch)
	for i, doc := range docs {
		meta := ch.Meta().Resources[doc.Name]
		blob, err := os.Open(filepath.Join(ch.Path, meta.Path))
		c.Assert(err, jc.ErrorIsNil)
		docs[i].Revision = addResource(c, store, entity, channel, doc, blob)
	}
	return docs
}

func addResource(c *gc.C, store *Store, entity *mongodoc.Entity, channel params.Channel, doc *mongodoc.Resource, blobReader io.Reader) int {
	revision := doc.Revision + 1
	if blobReader != nil {
		blob := ResourceBlob{
			Reader:      blobReader,
			Fingerprint: doc.Fingerprint,
			Size:        doc.Size,
		}
		err := store.addResource(entity, doc.Name, blob, revision)
		c.Assert(err, jc.ErrorIsNil)
	} else {
		doc.Revision = revision
		err := store.insertResource(doc)
		c.Assert(err, jc.ErrorIsNil)
	}
	if channel != params.UnpublishedChannel {
		err := store.setResource(entity, channel, doc.Name, revision)
		c.Assert(err, jc.ErrorIsNil)
	}
	return revision
}

func extractResources(c *gc.C, cURL *charm.URL, ch *charm.CharmDir) []*mongodoc.Resource {
	copied := *cURL
	cURL = &copied
	cURL.Revision = -1
	cURL.Series = ""
	var docs []*mongodoc.Resource
	for _, meta := range ch.Meta().Resources {
		data, err := ioutil.ReadFile(filepath.Join(ch.Path, meta.Path))
		c.Assert(err, jc.ErrorIsNil)
		fp, err := resource.GenerateFingerprint(bytes.NewReader(data))
		c.Assert(err, jc.ErrorIsNil)
		doc := &mongodoc.Resource{
			CharmURL:    cURL,
			Name:        meta.Name,
			Revision:    0,
			Fingerprint: fp.Bytes(),
			Size:        int64(len(data)),
			BlobName:    bson.NewObjectId().Hex(),
			UploadTime:  time.Now().UTC(),
		}
		docs = append(docs, doc)
	}
	return docs
}

func checkResourceDocs(c *gc.C, docs, expected []*mongodoc.Resource) {
	mongodoc.SortResources(docs)
	for i, doc := range docs {
		adjustExpectedResource(doc, expected[i])
	}
	c.Check(docs, jc.DeepEquals, expected)
}

func adjustExpectedResource(doc, expected *mongodoc.Resource) {
	expected.BlobName = doc.BlobName
	expected.UploadTime = doc.UploadTime
}
