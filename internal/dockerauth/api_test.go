// Copyright 2018 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package dockerauth_test

import (
	jc "github.com/juju/testing/checkers"
	gc "gopkg.in/check.v1"

	"gopkg.in/juju/charmstore.v5/internal/dockerauth"
)

type APISuite struct{}

var _ = gc.Suite(&APISuite{})

var parseScopeTests = []struct {
	scope                      string
	expectResourceAccessRights []dockerauth.ResourceAccessRights
	expectError                string
}{{
	scope: "",
}, {
	scope:       "bad",
	expectError: `invalid resource scope "bad"`,
}, {
	scope: "repository:1/2/3/4:push,pull",
	expectResourceAccessRights: []dockerauth.ResourceAccessRights{
		{
			Type:    "repository",
			Name:    "1/2/3/4",
			Actions: []string{"push", "pull"},
		},
	},
}, {
	scope: "repository:1/2/3/4:push,pull bad repository:5/6/7/8: more:bad repository:9/10:push",
	expectResourceAccessRights: []dockerauth.ResourceAccessRights{
		{
			Type:    "repository",
			Name:    "1/2/3/4",
			Actions: []string{"push", "pull"},
		},
		{
			Type: "repository",
			Name: "5/6/7/8",
		},
		{
			Type:    "repository",
			Name:    "9/10",
			Actions: []string{"push"},
		},
	},
	expectError: `\[invalid resource scope "bad", invalid resource scope "more:bad"\]`,
}}

func (s *APISuite) TestParseScope(c *gc.C) {
	for i, test := range parseScopeTests {
		c.Logf("%d. %q", i, test.scope)
		ras, err := dockerauth.ParseScope(test.scope)
		c.Assert(ras, jc.DeepEquals, test.expectResourceAccessRights)
		if test.expectError == "" {
			c.Assert(err, gc.Equals, nil)
		} else {
			c.Assert(err, gc.ErrorMatches, test.expectError)
		}
	}
}
