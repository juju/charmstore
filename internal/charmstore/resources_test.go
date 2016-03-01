// Copyright 2016 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package charmstore // import "gopkg.in/juju/charmstore.v5-unstable/internal/charmstore"

import (
	"bytes"
	"io/ioutil"
	"path/filepath"
	"sort"
	//"strings"

	jc "github.com/juju/testing/checkers"
	gc "gopkg.in/check.v1"
	"gopkg.in/juju/charm.v6-unstable"
	"gopkg.in/juju/charm.v6-unstable/resource"
	//"gopkg.in/juju/charmrepo.v2-unstable/csclient/params"
	//"gopkg.in/mgo.v2/bson"

	//"gopkg.in/juju/charmstore.v5-unstable/elasticsearch"
	//"gopkg.in/juju/charmstore.v5-unstable/internal/blobstore"
	//"gopkg.in/juju/charmstore.v5-unstable/internal/mongodoc"
	//"gopkg.in/juju/charmstore.v5-unstable/internal/router"
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
}

func (s *ResourcesSuite) TestListResourcesBundle(c *gc.C) {
}

func (s *ResourcesSuite) TestListResourcesResourceNotFound(c *gc.C) {
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
		err = store.AddResource(curl, res)
		c.Assert(err, jc.ErrorIsNil)
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
