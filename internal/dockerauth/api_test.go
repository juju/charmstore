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
	"encoding/json"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"time"

	jwt "github.com/dgrijalva/jwt-go"
	"github.com/juju/idmclient"
	"github.com/juju/idmclient/idmtest"
	jujutesting "github.com/juju/testing"
	jc "github.com/juju/testing/checkers"
	"github.com/juju/testing/httptesting"
	"golang.org/x/net/context"
	gc "gopkg.in/check.v1"
	httprequest "gopkg.in/httprequest.v1"
	charm "gopkg.in/juju/charm.v6"
	"gopkg.in/juju/charm.v6/resource"
	"gopkg.in/juju/charmrepo.v3/csclient/params"
	"gopkg.in/macaroon-bakery.v2-unstable/bakery/checkers"
	"gopkg.in/macaroon-bakery.v2-unstable/httpbakery"
	macaroon "gopkg.in/macaroon.v2-unstable"

	"gopkg.in/juju/charmstore.v5/internal/charmstore"
	"gopkg.in/juju/charmstore.v5/internal/dockerauth"
	"gopkg.in/juju/charmstore.v5/internal/mongodoc"
	"gopkg.in/juju/charmstore.v5/internal/router"
	"gopkg.in/juju/charmstore.v5/internal/storetesting"
	"gopkg.in/juju/charmstore.v5/internal/v5"
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
		PublicKeyLocator: idmServer,
	}, map[string]charmstore.NewAPIHandlerFunc{
		"docker-registry": dockerauth.NewAPIHandler,
		"v5":              v5.NewAPIHandler,
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

	req, err := http.NewRequest("GET", "/docker-registry/token?service=myregistry&scope=repository:bob/test/test-resource:pull,push", nil)
	c.Assert(err, gc.Equals, nil)
	req.SetBasicAuth("alice", dockerAuthPassword(store, "bob/test/test-resource", "pull"))

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
		"name": "bob/test/test-resource",
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
	c.Assert(err, gc.ErrorMatches, `Get http://.*docker-registry/token\?service=myregistry&scope=repository:pull,push: invalid access rights in resource scope "repository:pull,push": invalid resource scope "repository:pull,push"`)
}

func (s *APISuite) TestPullAuthWithInfoFromAPI(c *gc.C) {
	cert, key := newCert(c)
	hnd, store, idmServer := s.newServer(c, cert, key)
	defer hnd.Close()
	defer store.Close()
	defer idmServer.Close()
	srv := httptest.NewServer(hnd)
	defer srv.Close()

	id := router.MustNewResolvedURL("~charmers/kubecharm-0", -1)
	err := store.AddCharmWithArchive(id, storetesting.NewCharm(&charm.Meta{
		Series: []string{"kubernetes"},
		Resources: map[string]resource.Meta{
			"someResource": {
				Name: "someResource",
				Type: resource.TypeDocker,
			},
		},
	}))
	c.Assert(err, gc.Equals, nil)
	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler: hnd,
		Method:  "POST",
		URL:     "/v5/" + id.URL.Path() + "/resource/someResource",
		JSONBody: params.DockerResourceUploadRequest{
			Digest: "sha256:d1d44afba88cabf44cccd8d9fde2daacba31e09e9b7e46526ba9c1e3b41c0a3b",
		},
		ExpectBody: params.ResourceUploadResponse{
			Revision: 0,
		},
		Do: bakeryDoAsUser(idmServer, "charmers"),
	})
	var respData []byte
	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler: hnd,
		Method:  "GET",
		URL:     "/v5/" + id.URL.Path() + "/resource/someResource/0",
		ExpectBody: httptesting.BodyAsserter(func(c *gc.C, m json.RawMessage) {
			respData = m
		}),
		Do: bakeryDoAsUser(idmServer, "charmers"),
	})
	var infoResp params.DockerInfoResponse
	err = json.Unmarshal(respData, &infoResp)
	c.Assert(err, gc.Equals, nil)

	req, err := http.NewRequest("GET", "/docker-registry/token?service=myregistry&scope=repository:charmers/kubecharm/someResource:pull,push", nil)
	c.Assert(err, gc.Equals, nil)
	req.SetBasicAuth(infoResp.Username, infoResp.Password)

	client := httprequest.Client{
		BaseURL: srv.URL,
	}
	var tokenResp dockerauth.TokenResponse
	err = client.Do(context.Background(), req, &tokenResp)
	c.Assert(err, gc.Equals, nil)
	tok, err := jwt.Parse(tokenResp.Token, func(_ *jwt.Token) (interface{}, error) {
		return key.Public(), nil
	})
	c.Assert(err, gc.Equals, nil)
	claims, ok := tok.Claims.(jwt.MapClaims)
	c.Assert(ok, gc.Equals, true)
	c.Assert(claims["access"], jc.DeepEquals, []interface{}{map[string]interface{}{
		"type": "repository",
		"name": "charmers/kubecharm/someResource",
		"actions": []interface{}{
			"pull",
		},
	}})
}

func dockerAuthPassword(store *charmstore.Store, repoName, op string) string {
	m, err := store.Bakery.NewMacaroon([]checkers.Caveat{
		{Condition: "is-docker-repo " + repoName},
		checkers.AllowCaveat(op),
		checkers.TimeBeforeCaveat(time.Now().Add(time.Minute)),
	})
	if err != nil {
		panic(err)
	}
	b, err := macaroon.Slice{m}.MarshalBinary()
	if err != nil {
		panic(err)
	}
	return base64.RawStdEncoding.EncodeToString(b)
}

func bakeryDoAsUser(idm *idmtest.Server, user string) func(*http.Request) (*http.Response, error) {
	return bakeryDo(idm.Client(user))
}

func bakeryDo(client *httpbakery.Client) func(*http.Request) (*http.Response, error) {
	if client == nil {
		client = httpbakery.NewClient()
	}
	return func(req *http.Request) (*http.Response, error) {
		if req.Body == nil {
			return client.Do(req)
		}
		body := req.Body.(io.ReadSeeker)
		req.Body = nil
		return client.DoWithBody(req, body)
	}
}
