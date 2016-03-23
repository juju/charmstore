// Copyright 2016 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package charmstore // import "gopkg.in/juju/charmstore.v5-unstable/internal/charmstore"

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
	"gopkg.in/mgo.v2/bson"

	"gopkg.in/juju/charmstore.v5-unstable/internal/mongodoc"
	"gopkg.in/juju/charmstore.v5-unstable/internal/storetesting"
)

type ResourcesSuite struct {
	commonSuite
}

var _ = gc.Suite(&ResourcesSuite{})

func (s *ResourcesSuite) TestListResourcesCharmWithResources(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()
	curl := charm.MustParseURL("cs:~charmers/xenial/starsay-3")
	entity, expected := addCharmWithResources(c, store, curl)
	mongodoc.SortResources(expected)

	resources, err := store.ListResources(entity)
	c.Assert(err, jc.ErrorIsNil)

	mongodoc.SortResources(resources)
	c.Check(resources, jc.DeepEquals, expected)
}

func (s *ResourcesSuite) TestListResourcesCharmWithoutResources(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()
	curl := charm.MustParseURL("cs:~charmers/precise/wordpress-23")
	resolvedURL := MustParseResolvedURL(curl.String())
	ch := storetesting.Charms.CharmDir(curl.Name)
	err := store.AddCharmWithArchive(resolvedURL, ch)
	c.Assert(err, jc.ErrorIsNil)
	entity, err := store.FindEntity(resolvedURL, nil)
	c.Assert(err, jc.ErrorIsNil)

	resources, err := store.ListResources(entity)
	c.Assert(err, jc.ErrorIsNil)

	c.Check(resources, gc.HasLen, 0)
}

func (s *ResourcesSuite) TestListResourcesBundle(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()
	curl := charm.MustParseURL("cs:~charmers/bundle/wordpress-simple-0")
	resolvedURL := MustParseResolvedURL(curl.String())
	b := storetesting.Charms.BundleDir(curl.Name)
	s.addRequiredCharms(c, b)
	err := store.AddBundleWithArchive(resolvedURL, b)
	c.Assert(err, jc.ErrorIsNil)
	entity, err := store.FindEntity(resolvedURL, nil)
	c.Assert(err, jc.ErrorIsNil)

	_, err = store.ListResources(entity)

	c.Check(err, gc.ErrorMatches, `bundles do not have resources`)
}

func (s *ResourcesSuite) TestListResourcesResourceNotFound(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()
	curl := charm.MustParseURL("cs:~charmers/xenial/starsay-3")
	resolvedURL := MustParseResolvedURL(curl.String())
	ch := storetesting.Charms.CharmDir(curl.Name)
	expected := extractResources(c, curl, ch)
	mongodoc.SortResources(expected)
	expected[0] = &mongodoc.Resource{
		CharmURL: expected[0].CharmURL,
		Name:     expected[0].Name,
	}
	err := store.AddCharmWithArchive(resolvedURL, ch)
	c.Assert(err, jc.ErrorIsNil)
	entity, err := store.FindEntity(resolvedURL, nil)
	c.Assert(err, jc.ErrorIsNil)
	expected[1].Revision = addResource(c, store, entity, expected[1], nil)
	expected[2].Revision = addResource(c, store, entity, expected[2], nil)

	docs, err := store.ListResources(entity)
	c.Assert(err, jc.ErrorIsNil)

	mongodoc.SortResources(docs)
	c.Check(docs, jc.DeepEquals, expected)
}

func addCharmWithResources(c *gc.C, store *Store, curl *charm.URL) (*mongodoc.Entity, []*mongodoc.Resource) {
	resolvedURL := MustParseResolvedURL(curl.String())
	ch := storetesting.Charms.CharmDir(curl.Name)
	err := store.AddCharmWithArchive(resolvedURL, ch)
	c.Assert(err, jc.ErrorIsNil)

	entity, err := store.FindEntity(resolvedURL, nil)
	c.Assert(err, jc.ErrorIsNil)

	docs := extractResources(c, curl, ch)
	for i, doc := range docs {
		meta := ch.Meta().Resources[doc.Name]
		blob, err := os.Open(filepath.Join(ch.Path, meta.Path))
		c.Assert(err, jc.ErrorIsNil)
		docs[i].Revision = addResource(c, store, entity, doc, blob)
	}
	return entity, docs
}

func addResource(c *gc.C, store *Store, entity *mongodoc.Entity, doc *mongodoc.Resource, blob io.Reader) int {
	revision := doc.Revision + 1
	var err error
	if blob != nil {
		err := store.addResource(entity, doc, blob, revision)
		c.Assert(err, jc.ErrorIsNil)
	} else {
		doc.Revision = revision
		err := store.insertResource(entity, doc)
		c.Assert(err, jc.ErrorIsNil)
	}
	err = store.setResource(entity, doc.Name, revision)
	c.Assert(err, jc.ErrorIsNil)
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
