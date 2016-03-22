// Copyright 2016 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package mongodoc_test // import "gopkg.in/juju/charmstore.v5-unstable/internal/mongodoc"

import (
	jc "github.com/juju/testing/checkers"
	gc "gopkg.in/check.v1"
	"gopkg.in/juju/charm.v6-unstable"
	"gopkg.in/mgo.v2/bson"

	"gopkg.in/juju/charmstore.v5-unstable/internal/mongodoc"
)

const fingerprint = "0123456789abcdef0123456789abcdef0123456789abcdef"

type ResourceSuite struct{}

var _ = gc.Suite(&ResourceSuite{})

func (s *ResourceSuite) TestNewResourceQuery(c *gc.C) {
	cURL := charm.MustParseURL("cs:trusty/spam-2")

	query := mongodoc.NewResourceQuery(cURL, "eggs", 3)

	c.Check(query, jc.DeepEquals, bson.D{
		{"unresolved-charm-url", charm.MustParseURL("cs:spam")},
		{"name", "eggs"},
		{"revision", 3},
	})
}

func (s *ResourceSuite) TestValidateFull(c *gc.C) {
	doc := mongodoc.Resource{
		CharmURL:    charm.MustParseURL("cs:spam"),
		Name:        "spam",
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

func (s *ResourceSuite) TestValidateMissingCharmURL(c *gc.C) {
	doc := mongodoc.Resource{
		CharmURL:    nil,
		Name:        "spam",
		Revision:    1,
		Fingerprint: []byte(fingerprint),
		Size:        12,
	}

	err := doc.Validate()

	c.Check(err, gc.ErrorMatches, `missing charm URL`)
}

func (s *ResourceSuite) TestValidateUnexpectedCharmRevision(c *gc.C) {
	doc := mongodoc.Resource{
		CharmURL:    charm.MustParseURL("cs:spam-2"),
		Name:        "spam",
		Revision:    1,
		Fingerprint: []byte(fingerprint),
		Size:        12,
	}

	err := doc.Validate()

	c.Check(err, gc.ErrorMatches, `resolved charm URLs not supported \(got revision 2\)`)
}

func (s *ResourceSuite) TestValidateUnexpectedCharmSeries(c *gc.C) {
	doc := mongodoc.Resource{
		CharmURL:    charm.MustParseURL("cs:trusty/spam"),
		Name:        "spam",
		Revision:    1,
		Fingerprint: []byte(fingerprint),
		Size:        12,
	}

	err := doc.Validate()

	c.Check(err, gc.ErrorMatches, `series should not be set \(got "trusty"\)`)
}

func (s *ResourceSuite) TestValidateMissingName(c *gc.C) {
	doc := mongodoc.Resource{
		CharmURL:    charm.MustParseURL("cs:spam"),
		Name:        "",
		Revision:    1,
		Fingerprint: []byte(fingerprint),
		Size:        12,
	}

	err := doc.Validate()

	c.Check(err, gc.ErrorMatches, `missing name`)
}

func (s *ResourceSuite) TestValidateNegativeRevision(c *gc.C) {
	doc := mongodoc.Resource{
		CharmURL:    charm.MustParseURL("cs:spam"),
		Name:        "spam",
		Revision:    -1,
		Fingerprint: []byte(fingerprint),
		Size:        12,
	}

	err := doc.Validate()

	c.Check(err, gc.ErrorMatches, `got negative revision -1`)
}

func (s *ResourceSuite) TestValidateMissingFingerprint(c *gc.C) {
	doc := mongodoc.Resource{
		CharmURL:    charm.MustParseURL("cs:spam"),
		Name:        "spam",
		Revision:    1,
		Fingerprint: nil,
		Size:        0,
	}

	err := doc.Validate()

	c.Check(err, gc.ErrorMatches, `missing fingerprint`)
}

func (s *ResourceSuite) TestValidateBadFingerprint(c *gc.C) {
	doc := mongodoc.Resource{
		CharmURL:    charm.MustParseURL("cs:spam"),
		Name:        "spam",
		Revision:    1,
		Fingerprint: []byte(fingerprint + "0"),
		Size:        12,
	}

	err := doc.Validate()

	c.Check(err, gc.ErrorMatches, `bad fingerprint.*`)
}

func (s *ResourceSuite) TestValidateNegativeSize(c *gc.C) {
	doc := mongodoc.Resource{
		CharmURL:    charm.MustParseURL("cs:spam"),
		Name:        "spam",
		Revision:    1,
		Fingerprint: []byte(fingerprint),
		Size:        -1,
	}

	err := doc.Validate()

	c.Check(err, gc.ErrorMatches, `got negative size -1`)
}
