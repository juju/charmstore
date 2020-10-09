// Copyright 2016 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package mongodoc_test

import (
	"time"

	gc "gopkg.in/check.v1"
	"gopkg.in/juju/charmstore.v5/internal/charm"

	"gopkg.in/juju/charmstore.v5/internal/mongodoc"
)

const fakeBlobHash = "0123456789abcdef0123456789abcdef0123456789abcdef"

var _ = gc.Suite(&ResourceSuite{})

type ResourceSuite struct{}

var validateTests = []struct {
	about       string
	resource    *mongodoc.Resource
	expectError string
}{{
	about: "good resource",
	resource: &mongodoc.Resource{
		BaseURL:    charm.MustParseURL("cs:spam"),
		Name:       "spam",
		Revision:   1,
		BlobHash:   fakeBlobHash,
		Size:       12,
		UploadTime: time.Now().UTC(),
	},
}, {
	about:       "nil",
	expectError: "no document",
}, {
	about:       "zero value",
	resource:    &mongodoc.Resource{},
	expectError: ".*",
}, {
	about: "no url",
	resource: &mongodoc.Resource{
		Name:       "spam",
		Revision:   1,
		BlobHash:   fakeBlobHash,
		Size:       12,
		UploadTime: time.Now().UTC(),
	},
	expectError: "missing charm URL",
}, {
	about: "url with revision",
	resource: &mongodoc.Resource{
		BaseURL:    charm.MustParseURL("cs:spam-1"),
		Name:       "spam",
		Revision:   1,
		BlobHash:   fakeBlobHash,
		Size:       12,
		UploadTime: time.Now().UTC(),
	},
	expectError: `resolved charm URLs not supported \(got revision 1\)`,
}, {
	about: "url with series",
	resource: &mongodoc.Resource{
		BaseURL:    charm.MustParseURL("cs:trusty/spam"),
		Name:       "spam",
		Revision:   1,
		BlobHash:   fakeBlobHash,
		Size:       12,
		UploadTime: time.Now().UTC(),
	},
	expectError: `series should not be set \(got "trusty"\)`,
}, {
	about: "no name",
	resource: &mongodoc.Resource{
		BaseURL:    charm.MustParseURL("cs:spam"),
		Revision:   1,
		BlobHash:   fakeBlobHash,
		Size:       12,
		UploadTime: time.Now().UTC(),
	},
	expectError: `missing name`,
}, {
	about: "invalid revision",
	resource: &mongodoc.Resource{
		BaseURL:    charm.MustParseURL("cs:spam"),
		Name:       "spam",
		Revision:   -1,
		BlobHash:   fakeBlobHash,
		Size:       12,
		UploadTime: time.Now().UTC(),
	},
	expectError: `got negative revision -1`,
}, {
	about: "no blob hash",
	resource: &mongodoc.Resource{
		BaseURL:    charm.MustParseURL("cs:spam"),
		Name:       "spam",
		Revision:   0,
		Size:       12,
		UploadTime: time.Now().UTC(),
	},
	expectError: `missing blob hash`,
}, {
	about: "invalid size",
	resource: &mongodoc.Resource{
		BaseURL:    charm.MustParseURL("cs:spam"),
		Name:       "spam",
		Revision:   0,
		BlobHash:   fakeBlobHash,
		Size:       -2,
		UploadTime: time.Now().UTC(),
	},
	expectError: `got negative size -2`,
}, {
	about: "bad time",
	resource: &mongodoc.Resource{
		BaseURL:  charm.MustParseURL("cs:spam"),
		Name:     "spam",
		Revision: 0,
		BlobHash: fakeBlobHash,
		Size:     12,
	},
	expectError: `missing upload timestamp`,
}}

func (s *ResourceSuite) TestValidate(c *gc.C) {
	for i, test := range validateTests {
		c.Logf("%d. %s", i, test.about)
		err := test.resource.Validate()
		if test.expectError == "" {
			c.Assert(err, gc.Equals, nil)
			continue
		}
		c.Assert(err, gc.ErrorMatches, test.expectError)
	}
}
