// Copyright 2018 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package dockerauth_test

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
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
	cert, key := newCert(c)

	hnd, err := dockerauth.NewAPIHandler(charmstore.APIHandlerParams{
		ServerParams: charmstore.ServerParams{
			DockerRegistryAuthKey: key,
			DockerRegistryAuthCertificates: []*x509.Certificate{
				cert,
			},
		},
	})
	c.Assert(err, gc.Equals, nil)
	srv := httptest.NewServer(hnd)
	defer srv.Close()

	client := httprequest.Client{
		BaseURL: srv.URL,
	}
	var resp dockerauth.TokenResponse
	err = client.Get(context.Background(), "/token?service=myregistry&scope=repository:myrepo:pull", &resp)
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

func newCert(c *gc.C) (_ *x509.Certificate, key crypto.Signer) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	c.Assert(err, gc.Equals, nil)
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: "test",
		},
	}
	raw, err := x509.CreateCertificate(rand.Reader, &template, &template, key.(crypto.Signer).Public(), key)
	c.Assert(err, gc.Equals, nil)
	cert, err := x509.ParseCertificate(raw)
	c.Assert(err, gc.Equals, nil)
	return cert, key
}
