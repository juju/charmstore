// Copyright 2016 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package mongodoc_test // import "gopkg.in/juju/charmstore.v5-unstable/internal/mongodoc"

import (
	"bytes"

	jc "github.com/juju/testing/checkers"
	gc "gopkg.in/check.v1"
	"gopkg.in/juju/charm.v6-unstable"
	"gopkg.in/juju/charm.v6-unstable/resource"

	"gopkg.in/juju/charmstore.v5-unstable/internal/mongodoc"
)

type ResourceSuite struct{}

var _ = gc.Suite(&ResourceSuite{})

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

	doc := mongodoc.Resource{
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
