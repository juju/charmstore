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
	"encoding/base64"
	"math/big"
	"net/http"
	"net/http/httptest"

	jwt "github.com/dgrijalva/jwt-go"
	"github.com/juju/idmclient"
	"github.com/juju/idmclient/idmtest"
	jujutesting "github.com/juju/testing"
	jc "github.com/juju/testing/checkers"
	"golang.org/x/net/context"
	gc "gopkg.in/check.v1"
	httprequest "gopkg.in/httprequest.v1"
	charm "gopkg.in/juju/charm.v6"
	"gopkg.in/juju/charmrepo.v3/csclient/params"
	"gopkg.in/macaroon-bakery.v2-unstable/bakery/checkers"
	macaroon "gopkg.in/macaroon.v2-unstable"

	"gopkg.in/juju/charmstore.v5/internal/charmstore"
	"gopkg.in/juju/charmstore.v5/internal/dockerauth"
	"gopkg.in/juju/charmstore.v5/internal/mongodoc"
)

type APISuite struct {
	jujutesting.IsolatedMgoSuite
}

var _ = gc.Suite(&APISuite{})

func newCert(c *gc.C) (_ *x509.Certificate, key crypto.Signer) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	c.Assert(err, gc.Equals, nil)
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: "test",
		},
	}
	raw, err := x509.CreateCertificate(rand.Reader, &template, &template, key.Public(), key)
	c.Assert(err, gc.Equals, nil)
	cert, err := x509.ParseCertificate(raw)
	c.Assert(err, gc.Equals, nil)
	return cert, key
}

func (s *APISuite) newServer(c *gc.C, cert *x509.Certificate, key crypto.Signer) (*charmstore.Server, *charmstore.Store, *idmtest.Server) {
	idmServer := idmtest.NewServer()
	idmServer.AddUser("charmstore-agent")
	db := s.Session.DB("docker-registry-test")
	srv, err := charmstore.NewServer(db, nil, charmstore.ServerParams{
		AgentUsername:         "charmstore-agent",
		AgentKey:              idmServer.UserPublicKey("charmstore-agent"),
		DockerRegistryAuthKey: key,
		DockerRegistryAuthCertificates: []*x509.Certificate{
			cert,
		},
		IdentityLocation: idmServer.URL.String(),
	}, map[string]charmstore.NewAPIHandlerFunc{
		"docker-registry": dockerauth.NewAPIHandler,
	})
	c.Assert(err, gc.Equals, nil)
	return srv, srv.Pool().Store(), idmServer
}

func (s *APISuite) TestValidMacaroonToken(c *gc.C) {
	cert, key := newCert(c)
	hnd, store, idmServer := s.newServer(c, cert, key)
	defer hnd.Close()
	defer store.Close()
	defer idmServer.Close()
	srv := httptest.NewServer(hnd)
	defer srv.Close()

	err := store.DB.BaseEntities().Insert(&mongodoc.BaseEntity{
		URL: &charm.URL{
			Schema:   "cs",
			User:     "bob",
			Name:     "test",
			Revision: -1,
		},
		ChannelACLs: map[params.Channel]mongodoc.ACL{
			params.StableChannel: {
				Read:  []string{"alice", "bob"},
				Write: []string{"bob"},
			},
		},
	})
	c.Assert(err, gc.Equals, nil)

	m, err := store.Bakery.NewMacaroon([]checkers.Caveat{
		idmclient.UserDeclaration("alice"),
	})
	c.Assert(err, gc.Equals, nil)

	ms := macaroon.Slice{m}
	b, err := ms.MarshalBinary()
	c.Assert(err, gc.Equals, nil)

	req, err := http.NewRequest("GET", "/docker-registry/token?service=myregistry&scope=repository:bob/test/stable/test-resource:pull,push", nil)
	c.Assert(err, gc.Equals, nil)
	req.SetBasicAuth("alice", base64.RawStdEncoding.EncodeToString(b))

	client := httprequest.Client{
		BaseURL: srv.URL,
	}
	var resp dockerauth.TokenResponse
	err = client.Do(context.Background(), req, &resp)
	c.Assert(err, gc.Equals, nil)
	tok, err := jwt.Parse(resp.Token, func(_ *jwt.Token) (interface{}, error) {
		return key.Public(), nil
	})
	c.Assert(err, gc.Equals, nil)
	claims, ok := tok.Claims.(jwt.MapClaims)
	c.Assert(ok, gc.Equals, true)
	c.Assert(claims["access"], jc.DeepEquals, []interface{}{map[string]interface{}{
		"type": "repository",
		"name": "bob/test/stable/test-resource",
		"actions": []interface{}{
			"pull",
		},
	}})
}

func (s *APISuite) TestUnauthenticatedToken(c *gc.C) {
	cert, key := newCert(c)
	hnd, store, idmServer := s.newServer(c, cert, key)
	defer hnd.Close()
	defer store.Close()
	defer idmServer.Close()
	srv := httptest.NewServer(hnd)
	defer srv.Close()

	client := httprequest.Client{
		BaseURL: srv.URL,
	}
	var resp dockerauth.TokenResponse
	err := client.Get(context.Background(), "/docker-registry/token?service=myregistry&scope=repository:myrepo:pull,push", &resp)
	c.Assert(err, gc.Equals, nil)
	tok, err := jwt.Parse(resp.Token, func(_ *jwt.Token) (interface{}, error) {
		return key.Public(), nil
	})
	c.Assert(err, gc.Equals, nil)
	claims, ok := tok.Claims.(jwt.MapClaims)
	c.Assert(ok, gc.Equals, true)
	c.Assert(claims["access"], jc.DeepEquals, []interface{}{})
}

func (s *APISuite) TestInvalidScope(c *gc.C) {
	cert, key := newCert(c)
	hnd, store, idmServer := s.newServer(c, cert, key)
	defer hnd.Close()
	defer store.Close()
	defer idmServer.Close()
	srv := httptest.NewServer(hnd)
	defer srv.Close()

	err := store.DB.BaseEntities().Insert(&mongodoc.BaseEntity{
		URL: &charm.URL{
			Schema:   "cs",
			User:     "bob",
			Name:     "test",
			Revision: -1,
		},
		ChannelACLs: map[params.Channel]mongodoc.ACL{
			params.StableChannel: {
				Read:  []string{"alice", "bob"},
				Write: []string{"bob"},
			},
		},
	})
	c.Assert(err, gc.Equals, nil)

	m, err := store.Bakery.NewMacaroon([]checkers.Caveat{
		idmclient.UserDeclaration("alice"),
	})
	c.Assert(err, gc.Equals, nil)

	ms := macaroon.Slice{m}
	b, err := ms.MarshalBinary()
	c.Assert(err, gc.Equals, nil)

	req, err := http.NewRequest("GET", "/docker-registry/token?service=myregistry&scope=repository:pull,push", nil)
	c.Assert(err, gc.Equals, nil)
	req.SetBasicAuth("alice", base64.RawStdEncoding.EncodeToString(b))

	client := httprequest.Client{
		BaseURL: srv.URL,
	}
	var resp dockerauth.TokenResponse
	err = client.Do(context.Background(), req, &resp)
	c.Assert(err, gc.ErrorMatches, `Get .*/docker-registry/token\?service=myregistry&scope=repository:pull,push: invalid resource scope "repository:pull,push"`)
}
