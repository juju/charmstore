// Copyright 2016 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package charmstore // import "gopkg.in/juju/charmstore.v5-unstable/internal/charmstore"

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	jc "github.com/juju/testing/checkers"
	gc "gopkg.in/check.v1"
	"gopkg.in/juju/charm.v6-unstable"
	"gopkg.in/juju/charm.v6-unstable/resource"

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
	resource.Sort(expected)

	resources, err := store.ListResources(entity)
	c.Assert(err, jc.ErrorIsNil)

	resource.Sort(resources)
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
	expected := extractResources(c, ch)
	resource.Sort(expected)
	expected[0] = resource.Resource{
		Meta:   expected[0].Meta,
		Origin: resource.OriginUpload,
	}
	err := store.AddCharmWithArchive(resolvedURL, ch)
	c.Assert(err, jc.ErrorIsNil)
	entity, err := store.FindEntity(resolvedURL, nil)
	c.Assert(err, jc.ErrorIsNil)
	expected[1].Revision = addResource(c, store, entity, expected[1], nil)
	expected[2].Revision = addResource(c, store, entity, expected[2], nil)

	resources, err := store.ListResources(entity)
	c.Assert(err, jc.ErrorIsNil)

	resource.Sort(resources)
	c.Check(resources, jc.DeepEquals, expected)
}

func addCharmWithResources(c *gc.C, store *Store, curl *charm.URL) (*mongodoc.Entity, []resource.Resource) {
	resolvedURL := MustParseResolvedURL(curl.String())
	ch := storetesting.Charms.CharmDir(curl.Name)
	err := store.AddCharmWithArchive(resolvedURL, ch)
	c.Assert(err, jc.ErrorIsNil)

	entity, err := store.FindEntity(resolvedURL, nil)
	c.Assert(err, jc.ErrorIsNil)

	resources := extractResources(c, ch)
	for i, res := range resources {
		blob, err := os.Open(filepath.Join(ch.Path, res.Path))
		c.Assert(err, jc.ErrorIsNil)
		resources[i].Revision = addResource(c, store, entity, res, blob)
	}
	return entity, resources
}

func addResource(c *gc.C, store *Store, entity *mongodoc.Entity, res resource.Resource, blob io.Reader) int {
	revision := res.Revision + 1
	var err error
	if blob != nil {
		err := store.addResource(entity, res, blob, revision)
		c.Assert(err, jc.ErrorIsNil)
	} else {
		err := store.insertResource(entity, res, "", revision)
		c.Assert(err, jc.ErrorIsNil)
	}
	err = store.setResource(entity, res.Name, revision)
	c.Assert(err, jc.ErrorIsNil)
	return revision
}

func extractResources(c *gc.C, ch *charm.CharmDir) []resource.Resource {
	var resources []resource.Resource
	for _, meta := range ch.Meta().Resources {
		data, err := ioutil.ReadFile(filepath.Join(ch.Path, meta.Path))
		c.Assert(err, jc.ErrorIsNil)
		fp, err := resource.GenerateFingerprint(bytes.NewReader(data))
		c.Assert(err, jc.ErrorIsNil)
		res := resource.Resource{
			Meta:        meta,
			Origin:      resource.OriginStore,
			Revision:    0,
			Fingerprint: fp,
			Size:        int64(len(data)),
		}
		resources = append(resources, res)
	}
	return resources
}
