// Copyright 2016 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package charmstore

import (
	"strconv"
	"strings"
	"time"

	"github.com/juju/testing"
	jc "github.com/juju/testing/checkers"
	gc "gopkg.in/check.v1"
	"gopkg.in/juju/charm.v6-unstable"
	"gopkg.in/mgo.v2"

	"gopkg.in/juju/charmstore.v5-unstable/internal/mongodoc"
)

type resourceSuite struct {
	testing.IsolatedMgoSuite
	pool  *Pool
	store *Store
}

var _ = gc.Suite(&resourceSuite{})

func (s *resourceSuite) SetUpTest(c *gc.C) {
	s.IsolatedMgoSuite.SetUpTest(c)
	pool, err := NewPool(s.Session.DB("resource-test"), nil, nil, ServerParams{})
	c.Assert(err, gc.IsNil)
	s.store = pool.Store()
	pool.Close()
}

func (s *resourceSuite) TearDownTest(c *gc.C) {
	s.store.Close()
	s.IsolatedMgoSuite.TearDownTest(c)
}

func (s *resourceSuite) TestInsert(c *gc.C) {
	r := &mongodoc.Resource{
		BaseURL:    charm.MustParseURL("~bob/wordpress"),
		Name:       "resource-1",
		Revision:   0,
		BlobHash:   "123456",
		Size:       1,
		BlobName:   "res1",
		UploadTime: time.Now().UTC(),
	}

	// First insert works correctly.
	err := s.store.DB.Resources().Insert(r)
	c.Assert(err, jc.ErrorIsNil)

	// Attempting to insert the same revision fails.
	r.BlobHash = "78910"
	err = s.store.DB.Resources().Insert(r)
	c.Assert(mgo.IsDup(err), gc.Equals, true)

	// Inserting a different revision succeeds.
	r.Revision = 1
	err = s.store.DB.Resources().Insert(r)
	c.Assert(err, jc.ErrorIsNil)
}

var newResourceQueryTests = []struct {
	about           string
	url             *charm.URL
	name            string
	revision        int
	expectResources []*mongodoc.Resource
}{{
	about:    "without revision",
	url:      charm.MustParseURL("~bob/wordpress"),
	name:     "res",
	revision: -1,
	expectResources: []*mongodoc.Resource{{
		BaseURL:  charm.MustParseURL("~bob/wordpress"),
		Name:     "res",
		Revision: 0,
	}, {
		BaseURL:  charm.MustParseURL("~bob/wordpress"),
		Name:     "res",
		Revision: 1,
	}},
}, {
	about:    "with revision",
	url:      charm.MustParseURL("~bob/wordpress"),
	name:     "res",
	revision: 1,
	expectResources: []*mongodoc.Resource{{
		BaseURL:  charm.MustParseURL("~bob/wordpress"),
		Name:     "res",
		Revision: 1,
	}},
}}

func (s *resourceSuite) TestNewResourceQuery(c *gc.C) {
	for _, r := range []string{
		"~bob/wordpress|res|0",
		"~bob/wordpress|res|1",
		"~bob/wordpress|res2|0",
		"~bob/wordpress|res2|1",
		"~bob/mysql|res|0",
		"~alice/wordpress|res|0",
	} {
		parts := strings.SplitN(r, "|", 3)
		rev, err := strconv.Atoi(parts[2])
		c.Assert(err, jc.ErrorIsNil)
		err = s.store.DB.Resources().Insert(&mongodoc.Resource{
			BaseURL:  charm.MustParseURL(parts[0]),
			Name:     parts[1],
			Revision: rev,
		})
		c.Assert(err, jc.ErrorIsNil)
	}
	for i, test := range newResourceQueryTests {
		c.Logf("%d. %s", i, test.about)
		q := newResourceQuery(test.url, test.name, test.revision)
		var results []*mongodoc.Resource
		err := s.store.DB.Resources().Find(q).All(&results)
		c.Assert(err, jc.ErrorIsNil)
		sortResources(test.expectResources)
		sortResources(results)
		c.Assert(results, jc.DeepEquals, test.expectResources)
	}
}
