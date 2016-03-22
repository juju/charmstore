// Copyright 2016 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package mongodoc_test // import "gopkg.in/juju/charmstore.v5-unstable/internal/mongodoc"

import (
	"time"

	jc "github.com/juju/testing/checkers"
	gc "gopkg.in/check.v1"
	"gopkg.in/juju/charm.v6-unstable"
	"gopkg.in/juju/charmrepo.v2-unstable/csclient/params"
	"gopkg.in/mgo.v2/bson"

	"gopkg.in/juju/charmstore.v5-unstable/internal/mongodoc"
)

const fingerprint = "0123456789abcdef0123456789abcdef0123456789abcdef"

var (
	_ = gc.Suite(&ResourceSuite{})
	_ = gc.Suite(&ResourcesSuite{})
)

type ResourceSuite struct{}

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
		BlobName:    bson.NewObjectId().Hex(),
		UploadTime:  time.Now().UTC(),
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
		BlobName:    bson.NewObjectId().Hex(),
		UploadTime:  time.Now().UTC(),
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
		BlobName:    bson.NewObjectId().Hex(),
		UploadTime:  time.Now().UTC(),
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
		BlobName:    bson.NewObjectId().Hex(),
		UploadTime:  time.Now().UTC(),
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
		BlobName:    bson.NewObjectId().Hex(),
		UploadTime:  time.Now().UTC(),
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
		BlobName:    bson.NewObjectId().Hex(),
		UploadTime:  time.Now().UTC(),
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
		BlobName:    bson.NewObjectId().Hex(),
		UploadTime:  time.Now().UTC(),
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
		BlobName:    bson.NewObjectId().Hex(),
		UploadTime:  time.Now().UTC(),
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
		BlobName:    bson.NewObjectId().Hex(),
		UploadTime:  time.Now().UTC(),
	}

	err := doc.Validate()

	c.Check(err, gc.ErrorMatches, `got negative size -1`)
}

func (s *ResourceSuite) TestValidateMissingBlobName(c *gc.C) {
	doc := mongodoc.Resource{
		CharmURL:    charm.MustParseURL("cs:spam"),
		Name:        "spam",
		Revision:    1,
		Fingerprint: []byte(fingerprint),
		Size:        12,
		BlobName:    "",
		UploadTime:  time.Now().UTC(),
	}

	err := doc.Validate()

	c.Check(err, gc.ErrorMatches, `missing blob name`)
}

func (s *ResourceSuite) TestValidateMissingUploadTime(c *gc.C) {
	doc := mongodoc.Resource{
		CharmURL:    charm.MustParseURL("cs:spam"),
		Name:        "spam",
		Revision:    1,
		Fingerprint: []byte(fingerprint),
		Size:        12,
		BlobName:    bson.NewObjectId().Hex(),
		UploadTime:  time.Time{},
	}

	err := doc.Validate()

	c.Check(err, gc.ErrorMatches, `missing upload timestamp`)
}

type ResourcesSuite struct{}

func (s *ResourcesSuite) TestNewResourcesQuery(c *gc.C) {
	cURL := charm.MustParseURL("cs:trusty/spam-2")

	query := mongodoc.NewResourcesQuery(params.StableChannel, cURL)

	c.Check(query, jc.DeepEquals, bson.D{
		{"channel", params.StableChannel},
		{"resolved-charm-url", cURL},
	})
}

func (s *ResourcesSuite) TestValidateFull(c *gc.C) {
	doc := mongodoc.Resources{
		Channel:  params.StableChannel,
		CharmURL: charm.MustParseURL("cs:trusty/spam-2"),
		Revisions: map[string]int{
			"eggs": 1,
			"ham":  17,
		},
	}

	err := doc.Validate()

	c.Check(err, jc.ErrorIsNil)
}

func (s *ResourcesSuite) TestValidateZeroValue(c *gc.C) {
	var doc mongodoc.Resources

	err := doc.Validate()

	c.Check(err, gc.NotNil)
}

func (s *ResourcesSuite) TestValidateMissingChannel(c *gc.C) {
	doc := mongodoc.Resources{
		Channel:  "",
		CharmURL: charm.MustParseURL("cs:trusty/spam-2"),
		Revisions: map[string]int{
			"eggs": 1,
			"ham":  17,
		},
	}

	err := doc.Validate()

	c.Check(err, gc.ErrorMatches, `missing channel`)
}

func (s *ResourcesSuite) TestValidateMissingCharmURL(c *gc.C) {
	doc := mongodoc.Resources{
		Channel:  params.StableChannel,
		CharmURL: nil,
		Revisions: map[string]int{
			"eggs": 1,
			"ham":  17,
		},
	}

	err := doc.Validate()

	c.Check(err, gc.ErrorMatches, `missing charm URL`)
}

func (s *ResourcesSuite) TestValidateMissingCharmRevision(c *gc.C) {
	doc := mongodoc.Resources{
		Channel:  params.StableChannel,
		CharmURL: charm.MustParseURL("cs:trusty/spam"),
		Revisions: map[string]int{
			"eggs": 1,
			"ham":  17,
		},
	}

	err := doc.Validate()

	c.Check(err, gc.ErrorMatches, `unresolved charm URLs not supported`)
}

func (s *ResourcesSuite) TestValidateMissingCharmSeries(c *gc.C) {
	doc := mongodoc.Resources{
		Channel:  params.StableChannel,
		CharmURL: charm.MustParseURL("cs:spam-2"),
		Revisions: map[string]int{
			"eggs": 1,
			"ham":  17,
		},
	}

	err := doc.Validate()

	c.Check(err, gc.ErrorMatches, `series missing`)
}

func (s *ResourcesSuite) TestValidateMissingName(c *gc.C) {
	doc := mongodoc.Resources{
		Channel:  params.StableChannel,
		CharmURL: charm.MustParseURL("cs:trusty/spam-2"),
		Revisions: map[string]int{
			"eggs": 1,
			"":     42,
			"ham":  17,
		},
	}

	err := doc.Validate()

	c.Check(err, gc.ErrorMatches, `missing resource name`)
}

func (s *ResourcesSuite) TestValidateNegativeRevision(c *gc.C) {
	doc := mongodoc.Resources{
		Channel:  params.StableChannel,
		CharmURL: charm.MustParseURL("cs:trusty/spam-2"),
		Revisions: map[string]int{
			"eggs": -1,
			"ham":  17,
		},
	}

	err := doc.Validate()

	c.Check(err, gc.ErrorMatches, `got negative revision -1 for resource "eggs"`)
}
