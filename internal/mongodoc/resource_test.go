// Copyright 2015 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package mongodoc_test // import "gopkg.in/juju/charmstore.v5-unstable/internal/mongodoc"

import (
	"bytes"
	"fmt"

	jc "github.com/juju/testing/checkers"
	gc "gopkg.in/check.v1"
	"gopkg.in/juju/charm.v6-unstable"
	"gopkg.in/juju/charm.v6-unstable/resource"

	"gopkg.in/juju/charmstore.v5-unstable/internal/mongodoc"
)

type ResourceSuite struct{}

var _ = gc.Suite(&ResourceSuite{})

func (s *ResourceSuite) TestCheckResourceCharm(c *gc.C) {
	curl := charm.MustParseURL("cs:spam-2")
	entity := mongodoc.Entity{
		URL: curl,
	}

	err := mongodoc.CheckResourceCharm(entity)

	c.Check(err, jc.ErrorIsNil)
}

func (s *ResourceSuite) TestNewLatestResourceID(c *gc.C) {
	curl := charm.MustParseURL("cs:trust/spam-2")

	id := mongodoc.NewLatestResourceID(curl, "eggs", 3)

	c.Check(id, gc.Equals, "latest-resource#cs:trust/spam-2#eggs#3")
}

func (s *ResourceSuite) TestNewLatestResource(c *gc.C) {
	curl := charm.MustParseURL("cs:spam-2")
	entity := mongodoc.Entity{
		URL: curl,
		CharmMeta: &charm.Meta{
			Resources: map[string]resource.Meta{
				"ham":  resource.Meta{Name: "ham"},
				"eggs": resource.Meta{Name: "eggs"},
			},
		},
	}

	doc, err := mongodoc.NewLatestResource(entity, "eggs", 3)
	c.Assert(err, jc.ErrorIsNil)

	c.Check(doc, jc.DeepEquals, &mongodoc.LatestResource{
		DocID:    "latest-resource#cs:spam-2#eggs#3",
		CharmURL: curl,
		Resource: "eggs",
		Revision: 3,
	})
}

func (s *ResourceSuite) TestNewResourceID(c *gc.C) {
	curl := charm.MustParseURL("cs:trusty/spam-2")

	id := mongodoc.NewResourceID(curl, "eggs", 3)

	c.Check(id, gc.Equals, "resource#cs:spam#eggs#3")
}

func (s *ResourceSuite) TestResource2Doc(c *gc.C) {
	curl := charm.MustParseURL("cs:spam-2")
	res, expected := newResource(c, curl, "spam", "spamspamspam")

	doc, err := mongodoc.Resource2Doc(curl, res)
	c.Assert(err, jc.ErrorIsNil)

	c.Check(doc, jc.DeepEquals, &expected)
}

func (s *ResourceSuite) TestDoc2Resource(c *gc.C) {
	curl := charm.MustParseURL("cs:spam-2")
	expected, doc := newResource(c, curl, "spam", "spamspamspam")

	res, err := mongodoc.Doc2Resource(doc)
	c.Assert(err, jc.ErrorIsNil)

	c.Check(res, jc.DeepEquals, expected)
}

func newResource(c *gc.C, curl *charm.URL, name, data string) (resource.Resource, mongodoc.Resource) {
	curl.Series = ""
	curl.Revision = -1

	path := name + ".tgz"
	comment := "you really need this!!!"
	revision := 1

	fp, err := resource.GenerateFingerprint(bytes.NewReader([]byte(data)))
	c.Assert(err, jc.ErrorIsNil)
	size := int64(len(data))

	res := resource.Resource{
		Meta: resource.Meta{
			Name:        name,
			Type:        resource.TypeFile,
			Path:        path,
			Description: comment,
		},
		Origin:      resource.OriginStore,
		Revision:    revision,
		Fingerprint: fp,
		Size:        size,
	}
	err = res.Validate()
	c.Assert(err, jc.ErrorIsNil)

	id := fmt.Sprintf("resource#%s#%s#%d", curl, name, revision)
	doc := mongodoc.Resource{
		DocID:    id,
		CharmURL: curl,

		Name:        name,
		Type:        "file",
		Path:        path,
		Description: comment,

		Origin:      "store",
		Revision:    revision,
		Fingerprint: fp.Bytes(),
		Size:        size,
	}

	return res, doc
}
