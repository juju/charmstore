// Copyright 2016 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package mongodoc_test // import "gopkg.in/juju/charmstore.v5-unstable/internal/mongodoc"

import (
	jc "github.com/juju/testing/checkers"
	gc "gopkg.in/check.v1"
	"gopkg.in/juju/charm.v6-unstable"

	"gopkg.in/juju/charmstore.v5-unstable/internal/mongodoc"
)

const fingerprint = "0123456789abcdef0123456789abcdef0123456789abcdef"

type ResourceSuite struct{}

var _ = gc.Suite(&ResourceSuite{})

func (s *ResourceSuite) TestValidateFull(c *gc.C) {
	doc := mongodoc.Resource{
		CharmURL: charm.MustParseURL("cs:spam"),

		Name:        "spam",
		Type:        "file",
		Path:        "spam.tgz",
		Description: "you really need this!!!",

		Origin:      "store",
		Revision:    1,
		Fingerprint: []byte(fingerprint),
		Size:        12,
	}

	err := doc.Validate()

	c.Check(err, jc.ErrorIsNil)
}

func (s *ResourceSuite) TestValidateZeroValue(c *gc.C) {
	var doc mongodoc.Resource

	err := doc.Validate()

	c.Check(err, gc.NotNil)
}

func (s *ResourceSuite) TestValidateBadResource(c *gc.C) {
	doc := mongodoc.Resource{
		CharmURL: charm.MustParseURL("cs:spam"),

		Name: "",
		Type: "file",
		Path: "spam.tgz",

		Origin:      "store",
		Revision:    1,
		Fingerprint: []byte(fingerprint),
		Size:        12,
	}

	err := doc.Validate()

	c.Check(err, gc.NotNil)
}

func (s *ResourceSuite) TestValidateUnexpectedOrigin(c *gc.C) {
	doc := mongodoc.Resource{
		CharmURL: charm.MustParseURL("cs:spam"),

		Name: "spam",
		Type: "file",
		Path: "spam.tgz",

		Origin:      "upload",
		Revision:    0,
		Fingerprint: []byte(fingerprint),
		Size:        12,
	}

	err := doc.Validate()

	c.Check(err, gc.ErrorMatches, `unexpected origin "upload"`)
}

func (s *ResourceSuite) TestValidateMissingFingerprint(c *gc.C) {
	doc := mongodoc.Resource{
		CharmURL: charm.MustParseURL("cs:spam"),

		Name: "spam",
		Type: "file",
		Path: "spam.tgz",

		Origin:      "store",
		Revision:    1,
		Fingerprint: nil,
		Size:        0,
	}

	err := doc.Validate()

	c.Check(err, gc.ErrorMatches, `missing fingerprint`)
}

func (s *ResourceSuite) TestValidateMissingCharmURL(c *gc.C) {
	doc := mongodoc.Resource{
		CharmURL: nil,

		Name: "spam",
		Type: "file",
		Path: "spam.tgz",

		Origin:      "store",
		Revision:    1,
		Fingerprint: []byte(fingerprint),
		Size:        12,
	}

	err := doc.Validate()

	c.Check(err, gc.ErrorMatches, `missing charm URL`)
}

func (s *ResourceSuite) TestValidateUnexpectedRevision(c *gc.C) {
	doc := mongodoc.Resource{
		CharmURL: charm.MustParseURL("cs:spam-2"),

		Name: "spam",
		Type: "file",
		Path: "spam.tgz",

		Origin:      "store",
		Revision:    1,
		Fingerprint: []byte(fingerprint),
		Size:        12,
	}

	err := doc.Validate()

	c.Check(err, gc.ErrorMatches, `resolved charm URLs not supported \(got revision 2\)`)
}

func (s *ResourceSuite) TestValidateUnexpectedSeries(c *gc.C) {
	doc := mongodoc.Resource{
		CharmURL: charm.MustParseURL("cs:trusty/spam"),

		Name: "spam",
		Type: "file",
		Path: "spam.tgz",

		Origin:      "store",
		Revision:    1,
		Fingerprint: []byte(fingerprint),
		Size:        12,
	}

	err := doc.Validate()

	c.Check(err, gc.ErrorMatches, `series should not be set \(got "trusty"\)`)
}
