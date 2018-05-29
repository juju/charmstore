// Copyright 2018 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package dockerauth_test

import (
	"crypto/rand"
	"crypto/rsa"
	"net/http/httptest"

	jwt "github.com/dgrijalva/jwt-go"
	jc "github.com/juju/testing/checkers"
	"golang.org/x/net/context"
	gc "gopkg.in/check.v1"
	httprequest "gopkg.in/httprequest.v1"

	"gopkg.in/juju/charmstore.v5/internal/charmstore"
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

func (s *APISuite) TestToken(c *gc.C) {
	key, err := rsa.GenerateKey(rand.Reader, 512)
	c.Assert(err, gc.Equals, nil)
	hnd, err := dockerauth.NewAPIHandler(nil, &charmstore.ServerParams{
		DockerRegistryAuthorizerKey: key,
	}, "")
	c.Assert(err, gc.Equals, nil)
	srv := httptest.NewServer(hnd)
	defer srv.Close()

	client := httprequest.Client{
		BaseURL: srv.URL,
	}
	var resp dockerauth.TokenResponse
	err = client.Get(context.Background(), "/docker-registry/token?service=myregistry&scope=repository:myrepo:pull", &resp)
	c.Assert(err, gc.Equals, nil)
	tok, err := jwt.Parse(resp.Token, func(_ *jwt.Token) (interface{}, error) {
		return key.Public(), nil
	})
	c.Assert(err, gc.Equals, nil)
	claims, ok := tok.Claims.(jwt.MapClaims)
	c.Assert(ok, gc.Equals, true)
	c.Assert(claims["access"], jc.DeepEquals, []interface{}{
		map[string]interface{}{
			"type": "repository",
			"name": "myrepo",
			"actions": []interface{}{
				"pull",
			},
		},
	})
}
