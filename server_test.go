// Copyright 2014 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package charmstore_test

import (
	"net/http"
	"testing"

	jujutesting "github.com/juju/testing"
	gc "launchpad.net/gocheck"

	"github.com/juju/charmstore"
	"github.com/juju/charmstore/internal/storetesting"
	"github.com/juju/charmstore/params"
)

// These tests are copied (almost) verbatim from internal/charmstore/server_test.go

func TestPackage(t *testing.T) {
	jujutesting.MgoTestPackage(t, nil)
}

type ServerSuite struct {
	storetesting.IsolatedMgoSuite
}

var _ = gc.Suite(&ServerSuite{})

func (s *ServerSuite) TestNewServerWithNoVersions(c *gc.C) {
	h, err := charmstore.NewServer(s.Session.DB("foo"))
	c.Assert(err, gc.ErrorMatches, `charm store server must serve at least one version of the API`)
	c.Assert(h, gc.IsNil)
}

func (s *ServerSuite) TestNewServerWithUnregisteredVersion(c *gc.C) {
	h, err := charmstore.NewServer(s.Session.DB("foo"), "wrong")
	c.Assert(err, gc.ErrorMatches, `unknown version "wrong"`)
	c.Assert(h, gc.IsNil)
}

type versionResponse struct {
	Version string
	Path    string
}

func (s *ServerSuite) TestVersions(c *gc.C) {
	c.Assert(charmstore.Versions(), gc.DeepEquals, []string{"v4"})
}

func (s *ServerSuite) TestNewServerWithVersions(c *gc.C) {
	db := s.Session.DB("foo")

	h, err := charmstore.NewServer(db, charmstore.V4)
	c.Assert(err, gc.IsNil)

	storetesting.AssertJSONCall(c, h, "GET", "http://0.1.2.3/v4/debug", "", http.StatusInternalServerError, params.Error{
		Message: "method not implemented",
	})
	assertDoesNotServeVersion(c, h, "v3")
}

func assertServesVersion(c *gc.C, h http.Handler, vers string) {
	storetesting.AssertJSONCall(c, h, "GET", "http://0.1.2.3/"+vers+"/some/path", "", http.StatusOK, versionResponse{
		Version: vers,
		Path:    "/some/path",
	})
}

func assertDoesNotServeVersion(c *gc.C, h http.Handler, vers string) {
	rec := storetesting.DoRequest(c, h, "GET", "http://0.1.2.3/"+vers+"/debug", "")
	c.Assert(rec.Code, gc.Equals, http.StatusNotFound)
}
