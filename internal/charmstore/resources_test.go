// Copyright 2016 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package charmstore // import "gopkg.in/juju/charmstore.v5-unstable/internal/charmstore"

import (
	"bytes"
	"io/ioutil"
	"path/filepath"
	"sort"

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
	expected := addCharmWithResources(c, store, curl)
	sort.Sort(ByResourceName(expected))

	resources, err := store.ListResources(curl)
	c.Assert(err, jc.ErrorIsNil)

	sort.Sort(ByResourceName(resources))
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

	resources, err := store.ListResources(curl)
	c.Assert(err, jc.ErrorIsNil)

	c.Check(resources, gc.HasLen, 0)
}

func (s *ResourcesSuite) TestListResourcesCharmNotFound(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()
	curl := charm.MustParseURL("cs:~charmers/precise/wordpress-23")

	_, err := store.ListResources(curl)

	c.Check(err, gc.ErrorMatches, `entity not found`)
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

	_, err = store.ListResources(curl)

	c.Check(err, gc.ErrorMatches, `bundles do not have resources`)
}

func (s *ResourcesSuite) TestListResourcesLatestNotFound(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()
	curl := charm.MustParseURL("cs:~charmers/xenial/starsay-3")
	resolvedURL := MustParseResolvedURL(curl.String())
	ch := storetesting.Charms.CharmDir(curl.Name)
	err := store.AddCharmWithArchive(resolvedURL, ch)
	c.Assert(err, jc.ErrorIsNil)

	resources, err := store.ListResources(curl)
	c.Assert(err, jc.ErrorIsNil)

	sort.Sort(ByResourceName(resources))
	c.Check(resources, jc.DeepEquals, []resource.Resource{{
		Meta:   ch.Meta().Resources["for-install"],
		Origin: resource.OriginUpload,
	}, {
		Meta:   ch.Meta().Resources["for-store"],
		Origin: resource.OriginUpload,
	}, {
		Meta:   ch.Meta().Resources["for-upload"],
		Origin: resource.OriginUpload,
	}})
}

func (s *ResourcesSuite) TestListResourcesResourceNotFound(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()
	curl := charm.MustParseURL("cs:~charmers/xenial/starsay-3")
	resolvedURL := MustParseResolvedURL(curl.String())
	ch := storetesting.Charms.CharmDir(curl.Name)
	expected := extractResources(c, ch)
	sort.Sort(ByResourceName(expected))
	expected[0] = resource.Resource{
		Meta:   expected[0].Meta,
		Origin: resource.OriginUpload,
	}
	err := store.AddCharmWithArchive(resolvedURL, ch)
	c.Assert(err, jc.ErrorIsNil)
	entity, err := store.FindBestEntity(curl, nil)
	c.Assert(err, jc.ErrorIsNil)
	latest, err := mongodoc.NewLatestResource(entity, expected[0].Name, 1)
	c.Assert(err, jc.ErrorIsNil)
	err = store.DB.Resources().Insert(latest)
	c.Assert(err, jc.ErrorIsNil)
	err = store.AddResource(curl, expected[1])
	c.Assert(err, jc.ErrorIsNil)
	err = store.AddResource(curl, expected[2])
	c.Assert(err, jc.ErrorIsNil)

	resources, err := store.ListResources(curl)
	c.Assert(err, jc.ErrorIsNil)

	sort.Sort(ByResourceName(resources))
	c.Check(resources, jc.DeepEquals, expected)
}

func (s *ResourcesSuite) TestListResourcesBadDoc(c *gc.C) {
	store := s.newStore(c, false)
	defer store.Close()
	curl := charm.MustParseURL("cs:~charmers/xenial/starsay-3")
	resolvedURL := MustParseResolvedURL(curl.String())
	ch := storetesting.Charms.CharmDir(curl.Name)
	resources := extractResources(c, ch)
	sort.Sort(ByResourceName(resources))
	resources[0].Revision = 1
	err := store.AddCharmWithArchive(resolvedURL, ch)
	c.Assert(err, jc.ErrorIsNil)
	entity, err := store.FindBestEntity(curl, nil)
	c.Assert(err, jc.ErrorIsNil)
	latest, err := mongodoc.NewLatestResource(entity, resources[0].Name, 1)
	c.Assert(err, jc.ErrorIsNil)
	err = store.DB.Resources().Insert(latest)
	c.Assert(err, jc.ErrorIsNil)
	doc, err := mongodoc.Resource2Doc(curl, resources[0])
	c.Assert(err, jc.ErrorIsNil)
	doc.Type = "<bogus>"
	err = store.DB.Resources().Insert(doc)
	c.Assert(err, jc.ErrorIsNil)

	_, err = store.ListResources(curl)

	c.Check(err, gc.ErrorMatches, `.*got invalid data from DB.*`)
}

func (s *ResourcesSuite) TestAddResourceNotAddedYet(c *gc.C) {
}

func (s *ResourcesSuite) TestAddResourceReplaceExisting(c *gc.C) {
}

func (s *ResourcesSuite) TestAddResourceCharmNotFound(c *gc.C) {
}

func (s *ResourcesSuite) TestAddResourceBundle(c *gc.C) {
}

func (s *ResourcesSuite) TestAddResourceRevisionCollision(c *gc.C) {
}

func (s *ResourcesSuite) TestAddResourceInvalidInfo(c *gc.C) {
}

func (s *ResourcesSuite) TestAddResourceCharmWithoutResource(c *gc.C) {
}

func (s *ResourcesSuite) TestAddResourceMissingResourceRevision(c *gc.C) {
}

func (s *ResourcesSuite) TestAddResourceCleanUpOnInsertLatestFailure(c *gc.C) {
}

func addCharmWithResources(c *gc.C, store *Store, curl *charm.URL) []resource.Resource {
	resolvedURL := MustParseResolvedURL(curl.String())
	ch := storetesting.Charms.CharmDir(curl.Name)
	err := store.AddCharmWithArchive(resolvedURL, ch)
	c.Assert(err, jc.ErrorIsNil)

	resources := extractResources(c, ch)
	for _, res := range resources {
		err = store.AddResource(curl, res)
		c.Assert(err, jc.ErrorIsNil)
	}
	return resources
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

//func newResource(c *gc.C, name string, rev int, content string) resource.Resource {
//	fp, err := resource.GenerateFingerprint(strings.NewReader(content))
//	c.Assert(err, jc.ErrorIsNil)
//	res := resource.Resource{
//		Meta: resource.Meta{
//			Name:        name,
//			Type:        charmresource.TypeFile,
//			Path:        name + ".tgz",
//			Description: "resource " + name,
//		},
//		Origin:      resource.OriginStore,
//		Revision:    rev,
//		Fingerprint: fp,
//		Size:        int64(len(content)),
//	}
//	err = res.Validate()
//	c.Assert(err, jc.ErrorIsNil)
//
//	return res
//}

type ByResourceName []resource.Resource

func (sorted ByResourceName) Len() int           { return len(sorted) }
func (sorted ByResourceName) Swap(i, j int)      { sorted[i], sorted[j] = sorted[j], sorted[i] }
func (sorted ByResourceName) Less(i, j int) bool { return sorted[i].Name < sorted[j].Name }
