// Copyright 2014 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package v5_test // import "gopkg.in/juju/charmstore.v5-unstable/internal/v5"

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	jc "github.com/juju/testing/checkers"
	"github.com/juju/testing/httptesting"
	gc "gopkg.in/check.v1"
	"gopkg.in/errgo.v1"
	"gopkg.in/juju/charm.v6-unstable"
	"gopkg.in/juju/charmrepo.v2-unstable/csclient/params"
	"gopkg.in/macaroon-bakery.v1/bakery/checkers"
	"gopkg.in/macaroon-bakery.v1/httpbakery"
	"gopkg.in/macaroon.v1"

	"gopkg.in/juju/charmstore.v5-unstable/internal/charmstore"
	"gopkg.in/juju/charmstore.v5-unstable/internal/storetesting"
	"gopkg.in/juju/charmstore.v5-unstable/internal/v5"
)

func (s *commonSuite) AssertEndpointAuth(c *gc.C, p httptesting.JSONCallParams) {
	s.testNonMacaroonAuth(c, p)
	s.testMacaroonAuth(c, p)
}

func (s *commonSuite) testNonMacaroonAuth(c *gc.C, p httptesting.JSONCallParams) {
	p.Handler = s.noMacaroonSrv
	// Check that the request succeeds when provided with the
	// correct credentials.
	p.Username = "test-user"
	p.Password = "test-password"
	httptesting.AssertJSONCall(c, p)

	// Check that auth fails with no creds provided.
	p.Username = ""
	p.Password = ""
	p.ExpectStatus = http.StatusUnauthorized
	p.ExpectBody = params.Error{
		Message: "authentication failed: missing HTTP auth header",
		Code:    params.ErrUnauthorized,
	}
	httptesting.AssertJSONCall(c, p)

	// Check that auth fails with the wrong username provided.
	p.Username = "wrong"
	p.Password = "test-password"
	p.ExpectStatus = http.StatusUnauthorized
	p.ExpectBody = params.Error{
		Message: "invalid user name or password",
		Code:    params.ErrUnauthorized,
	}
	httptesting.AssertJSONCall(c, p)

	// Check that auth fails with the wrong password provided.
	p.Username = "test-user"
	p.Password = "test-password-wrong"
	p.ExpectStatus = http.StatusUnauthorized
	p.ExpectBody = params.Error{
		Message: "invalid user name or password",
		Code:    params.ErrUnauthorized,
	}
	httptesting.AssertJSONCall(c, p)
}

func (s *commonSuite) testMacaroonAuth(c *gc.C, p httptesting.JSONCallParams) {
	// Make a test third party caveat discharger.
	var checkedCaveats []string
	var mu sync.Mutex
	var dischargeError error
	s.discharge = func(cond string, arg string) ([]checkers.Caveat, error) {
		mu.Lock()
		defer mu.Unlock()
		checkedCaveats = append(checkedCaveats, cond+" "+arg)
		if dischargeError != nil {
			return nil, dischargeError
		}
		return []checkers.Caveat{
			checkers.DeclaredCaveat("username", "bob"),
		}, nil
	}
	p.Handler = s.srv

	client := httpbakery.NewHTTPClient()
	cookieJar := &cookieJar{CookieJar: client.Jar}
	client.Jar = cookieJar
	p.Do = bakeryDo(client)

	// Check that the call succeeds with simple auth.
	c.Log("simple auth sucess")
	p.Username = "test-user"
	p.Password = "test-password"
	httptesting.AssertJSONCall(c, p)
	c.Assert(checkedCaveats, gc.HasLen, 0)
	c.Assert(cookieJar.cookieURLs, gc.HasLen, 0)

	// Check that the call gives us the correct
	// "authentication denied response" without simple auth
	// and uses the third party checker
	// and that a cookie is stored at the correct location.
	// TODO when we allow admin access via macaroon creds,
	// change this test to expect success.
	c.Log("macaroon unauthorized error")
	p.Username, p.Password = "", ""
	p.ExpectStatus = http.StatusUnauthorized
	p.ExpectBody = params.Error{
		Message: `unauthorized: access denied for user "bob"`,
		Code:    params.ErrUnauthorized,
	}
	httptesting.AssertJSONCall(c, p)
	sort.Strings(checkedCaveats)
	c.Assert(checkedCaveats, jc.DeepEquals, []string{
		"is-authenticated-user ",
	})
	checkedCaveats = nil
	c.Assert(cookieJar.cookieURLs, gc.DeepEquals, []string{"http://somehost/"})

	// Check that the call fails with incorrect simple auth info.
	c.Log("simple auth error")
	p.Password = "bad-password"
	p.ExpectStatus = http.StatusUnauthorized
	p.ExpectBody = params.Error{
		Message: "authentication failed: missing HTTP auth header",
		Code:    params.ErrUnauthorized,
	}

	// Check that it fails when the discharger refuses the discharge.
	c.Log("macaroon discharge error")
	client = httpbakery.NewHTTPClient()
	dischargeError = fmt.Errorf("go away")
	p.Do = bakeryDo(client) // clear cookies
	p.Password = ""
	p.Username = ""
	p.ExpectError = `cannot get discharge from "https://[^"]*": third party refused discharge: cannot discharge: go away`
	httptesting.AssertJSONCall(c, p)
}

type cookieJar struct {
	cookieURLs []string
	http.CookieJar
}

func (j *cookieJar) SetCookies(url *url.URL, cookies []*http.Cookie) {
	url1 := *url
	url1.Host = "somehost"
	for _, cookie := range cookies {
		if cookie.Path != "" {
			url1.Path = cookie.Path
		}
		if cookie.Name != "macaroon-authn" {
			panic("unexpected cookie name: " + cookie.Name)
		}
	}
	j.cookieURLs = append(j.cookieURLs, url1.String())
	j.CookieJar.SetCookies(url, cookies)
}

func noInteraction(*url.URL) error {
	return fmt.Errorf("unexpected interaction required")
}

type authSuite struct {
	commonSuite
}

var _ = gc.Suite(&authSuite{})

func (s *authSuite) SetUpSuite(c *gc.C) {
	s.enableIdentity = true
	s.commonSuite.SetUpSuite(c)
}

var readAuthorizationTests = []struct {
	// about holds the test description.
	about string
	// username holds the authenticated user name returned by the discharger.
	// If empty, an anonymous user is returned.
	username string
	// groups holds group names the user is member of, as returned by the
	// discharger.
	groups []string
	// unpublishedReadPerm stores a list of users with read permissions on
	// on the unpublished entities.
	unpublishedReadPerm []string
	// edgeReadPerm stores a list of users with read permissions on the edge channel.
	edgeReadPerm []string
	// stableReadPerm stores a list of users with read permissions on the stable channel.
	stableReadPerm []string
	// channels contains a list of channels, to which the entity belongs.
	channels []params.Channel
	// expectStatus is the expected HTTP response status.
	// Defaults to 200 status OK.
	expectStatus int
	// expectBody holds the expected body of the HTTP response. If nil,
	// the body is not checked and the response is assumed to be ok.
	expectBody interface{}
}{{
	about:               "anonymous users are authorized",
	unpublishedReadPerm: []string{params.Everyone},
}, {
	about:               "everyone is authorized",
	username:            "dalek",
	unpublishedReadPerm: []string{params.Everyone},
}, {
	about:               "everyone and a specific user",
	username:            "dalek",
	unpublishedReadPerm: []string{params.Everyone, "janeway"},
}, {
	about:               "specific user authorized",
	username:            "who",
	unpublishedReadPerm: []string{"who"},
}, {
	about:               "multiple specific users authorized",
	username:            "picard",
	unpublishedReadPerm: []string{"kirk", "picard", "sisko"},
}, {
	about:        "nobody authorized",
	username:     "picard",
	expectStatus: http.StatusUnauthorized,
	expectBody: params.Error{
		Code:    params.ErrUnauthorized,
		Message: `unauthorized: access denied for user "picard"`,
	},
}, {
	about:               "access denied for user",
	username:            "kirk",
	unpublishedReadPerm: []string{"picard", "sisko"},
	expectStatus:        http.StatusUnauthorized,
	expectBody: params.Error{
		Code:    params.ErrUnauthorized,
		Message: `unauthorized: access denied for user "kirk"`,
	},
}, {
	about:               "everyone is authorized (user is member of groups)",
	username:            "dalek",
	groups:              []string{"group1", "group2"},
	unpublishedReadPerm: []string{params.Everyone},
}, {
	about:               "everyone and a specific group",
	username:            "dalek",
	groups:              []string{"group2", "group3"},
	unpublishedReadPerm: []string{params.Everyone, "group1"},
}, {
	about:               "specific group authorized",
	username:            "who",
	groups:              []string{"group1", "group42", "group2"},
	unpublishedReadPerm: []string{"group42"},
}, {
	about:               "multiple specific groups authorized",
	username:            "picard",
	groups:              []string{"group2"},
	unpublishedReadPerm: []string{"kirk", "group0", "group2"},
}, {
	about:        "no group authorized",
	username:     "picard",
	groups:       []string{"group1", "group2"},
	expectStatus: http.StatusUnauthorized,
	expectBody: params.Error{
		Code:    params.ErrUnauthorized,
		Message: `unauthorized: access denied for user "picard"`,
	},
}, {
	about:               "access denied for group",
	username:            "kirk",
	groups:              []string{"group1", "group2", "group3"},
	unpublishedReadPerm: []string{"picard", "sisko", "group42", "group47"},
	expectStatus:        http.StatusUnauthorized,
	expectBody: params.Error{
		Code:    params.ErrUnauthorized,
		Message: `unauthorized: access denied for user "kirk"`,
	},
}, {
	about:               "access provided through edge channel",
	username:            "kirk",
	groups:              []string{"group1", "group2", "group3"},
	unpublishedReadPerm: []string{"picard", "sisko", "group42", "group47"},
	edgeReadPerm:        []string{"group1"},
	channels:            []params.Channel{params.EdgeChannel},
}, {
	about:               "access provided through edge channel, but charm not published",
	username:            "kirk",
	groups:              []string{"group1", "group2", "group3"},
	unpublishedReadPerm: []string{"picard", "sisko", "group42", "group47"},
	edgeReadPerm:        []string{"group1"},
	expectStatus:        http.StatusUnauthorized,
	expectBody: params.Error{
		Code:    params.ErrUnauthorized,
		Message: `unauthorized: access denied for user "kirk"`,
	},
}, {
	about:               "access provided through stable channel",
	username:            "kirk",
	groups:              []string{"group1", "group2", "group3"},
	unpublishedReadPerm: []string{"picard", "sisko", "group42", "group47"},
	edgeReadPerm:        []string{"group12"},
	stableReadPerm:      []string{"group2"},
	channels:            []params.Channel{params.EdgeChannel, params.StableChannel},
}, {
	about:               "access provided through stable channel, but charm not published",
	username:            "kirk",
	groups:              []string{"group1", "group2", "group3"},
	unpublishedReadPerm: []string{"picard", "sisko", "group42", "group47"},
	edgeReadPerm:        []string{"group12"},
	stableReadPerm:      []string{"group2"},
	channels:            []params.Channel{params.EdgeChannel},
	expectStatus:        http.StatusUnauthorized,
	expectBody: params.Error{
		Code:    params.ErrUnauthorized,
		Message: `unauthorized: access denied for user "kirk"`,
	},
}, {
	about:               "access provided through edge channel, but charm on stable channel",
	username:            "kirk",
	groups:              []string{"group1", "group2", "group3"},
	unpublishedReadPerm: []string{"picard", "sisko", "group42", "group47"},
	edgeReadPerm:        []string{"group1"},
	stableReadPerm:      []string{"group11"},
	channels: []params.Channel{
		params.EdgeChannel,
		params.StableChannel,
	},
	expectStatus: http.StatusUnauthorized,
	expectBody: params.Error{
		Code:    params.ErrUnauthorized,
		Message: `unauthorized: access denied for user "kirk"`,
	},
}, {
	about:               "access provided through unpublished ACL, but charm on stable channel",
	username:            "kirk",
	groups:              []string{"group1", "group2", "group3"},
	unpublishedReadPerm: []string{"picard", "sisko", "group42", "group1"},
	stableReadPerm:      []string{"group11"},
	channels: []params.Channel{
		params.EdgeChannel,
		params.StableChannel,
	},
	expectStatus: http.StatusUnauthorized,
	expectBody: params.Error{
		Code:    params.ErrUnauthorized,
		Message: `unauthorized: access denied for user "kirk"`,
	},
}, {
	about:               "access provided through unpublished ACL, but charm on edge channel",
	username:            "kirk",
	groups:              []string{"group1", "group2", "group3"},
	unpublishedReadPerm: []string{"picard", "sisko", "group42", "group1"},
	edgeReadPerm:        []string{"group11"},
	channels: []params.Channel{
		params.EdgeChannel,
	},
	expectStatus: http.StatusUnauthorized,
	expectBody: params.Error{
		Code:    params.ErrUnauthorized,
		Message: `unauthorized: access denied for user "kirk"`,
	},
}}

func (s *authSuite) TestReadAuthorization(c *gc.C) {
	for i, test := range readAuthorizationTests {
		c.Logf("test %d: %s", i, test.about)

		s.discharge = dischargeForUser(test.username)
		s.idM.groups = map[string][]string{
			test.username: test.groups,
		}

		// Add a charm to the store, used for testing.
		rurl := newResolvedURL("~charmers/utopic/wordpress-42", -1)
		err := s.store.AddCharmWithArchive(rurl, storetesting.Charms.CharmDir("wordpress"))
		c.Assert(err, gc.IsNil)

		// publish the charm on any required channels.
		if len(test.channels) > 0 {
			err := s.store.Publish(rurl, nil, test.channels...)
			c.Assert(err, gc.IsNil)
		}

		// Change the ACLs for the testing charm.
		err = s.store.SetPerms(&rurl.URL, "unpublished.read", test.unpublishedReadPerm...)
		c.Assert(err, gc.IsNil)
		err = s.store.SetPerms(&rurl.URL, "edge.read", test.edgeReadPerm...)
		c.Assert(err, gc.IsNil)
		err = s.store.SetPerms(&rurl.URL, "stable.read", test.stableReadPerm...)
		c.Assert(err, gc.IsNil)

		// Define an helper function used to send requests and check responses.
		doRequest := func(path string, expectStatus int, expectBody interface{}) {
			rec := httptesting.DoRequest(c, httptesting.DoRequestParams{
				Handler: s.srv,
				Do:      bakeryDo(nil),
				URL:     storeURL(path),
			})
			if expectStatus == 0 {
				expectStatus = http.StatusOK
			}
			c.Assert(rec.Code, gc.Equals, expectStatus, gc.Commentf("body: %s", rec.Body))
			if expectBody != nil {
				c.Assert(rec.Body.String(), jc.JSONEquals, expectBody)
			}
		}

		// Perform meta and id requests.
		// Note that we use the full URL so that we test authorization specifically
		// on that entity without trying to look up the entity in the stable channel.
		doRequest("~charmers/utopic/wordpress-42/meta/archive-size", test.expectStatus, test.expectBody)
		doRequest("~charmers/utopic/wordpress-42/expand-id", test.expectStatus, test.expectBody)

		// Remove all entities from the store.
		_, err = s.store.DB.Entities().RemoveAll(nil)
		c.Assert(err, gc.IsNil)
	}
}

var writeAuthorizationTests = []struct {
	// about holds the test description.
	about string
	// username holds the authenticated user name returned by the discharger.
	// If empty, an anonymous user is returned.
	username string
	// groups holds group names the user is member of, as returned by the
	// discharger.
	groups []string
	// writePerm stores a list of users with write permissions.
	unpublishedWritePerm []string
	// edgeWritePerm stores a list of users with write permissions on the edge channel.
	edgeWritePerm []string
	// stableWritePerm stores a list of users with write permissions on the stable channel.
	stableWritePerm []string
	// channels contains a list of channels, to which the entity belongs.
	channels []params.Channel
	// expectStatus is the expected HTTP response status.
	// Defaults to 200 status OK.
	expectStatus int
	// expectBody holds the expected body of the HTTP response. If nil,
	// the body is not checked and the response is assumed to be ok.
	expectBody interface{}
}{{
	about:                "anonymous users are not authorized",
	unpublishedWritePerm: []string{"who"},
	expectStatus:         http.StatusUnauthorized,
	expectBody: params.Error{
		Code:    params.ErrUnauthorized,
		Message: "unauthorized: no username declared",
	},
}, {
	about:                "specific user authorized to write",
	username:             "dalek",
	unpublishedWritePerm: []string{"dalek"},
}, {
	about:                "multiple users authorized",
	username:             "sisko",
	unpublishedWritePerm: []string{"kirk", "picard", "sisko"},
}, {
	about:        "no users authorized",
	username:     "who",
	expectStatus: http.StatusUnauthorized,
	expectBody: params.Error{
		Code:    params.ErrUnauthorized,
		Message: `unauthorized: access denied for user "who"`,
	},
}, {
	about:                "specific user unauthorized",
	username:             "kirk",
	unpublishedWritePerm: []string{"picard", "sisko", "janeway"},
	expectStatus:         http.StatusUnauthorized,
	expectBody: params.Error{
		Code:    params.ErrUnauthorized,
		Message: `unauthorized: access denied for user "kirk"`,
	},
}, {
	about:                "access granted for group",
	username:             "picard",
	groups:               []string{"group1", "group2"},
	unpublishedWritePerm: []string{"group2"},
}, {
	about:                "multiple groups authorized",
	username:             "picard",
	groups:               []string{"group1", "group2"},
	unpublishedWritePerm: []string{"kirk", "group0", "group1", "group2"},
}, {
	about:        "no group authorized",
	username:     "picard",
	groups:       []string{"group1", "group2"},
	expectStatus: http.StatusUnauthorized,
	expectBody: params.Error{
		Code:    params.ErrUnauthorized,
		Message: `unauthorized: access denied for user "picard"`,
	},
}, {
	about:                "access denied for group",
	username:             "kirk",
	groups:               []string{"group1", "group2", "group3"},
	unpublishedWritePerm: []string{"picard", "sisko", "group42", "group47"},
	expectStatus:         http.StatusUnauthorized,
	expectBody: params.Error{
		Code:    params.ErrUnauthorized,
		Message: `unauthorized: access denied for user "kirk"`,
	},
}, {
	about:                "access provided through edge channel",
	username:             "kirk",
	groups:               []string{"group1", "group2", "group3"},
	unpublishedWritePerm: []string{"picard", "sisko", "group42", "group47"},
	edgeWritePerm:        []string{"group1"},
	channels:             []params.Channel{params.EdgeChannel},
}, {
	about:                "access provided through edge channel, but charm not published",
	username:             "kirk",
	groups:               []string{"group1", "group2", "group3"},
	unpublishedWritePerm: []string{"picard", "sisko", "group42", "group47"},
	edgeWritePerm:        []string{"group1"},
	expectStatus:         http.StatusUnauthorized,
	expectBody: params.Error{
		Code:    params.ErrUnauthorized,
		Message: `unauthorized: access denied for user "kirk"`,
	},
}, {
	about:                "access provided through stable channel",
	username:             "kirk",
	groups:               []string{"group1", "group2", "group3"},
	unpublishedWritePerm: []string{"picard", "sisko", "group42", "group47"},
	edgeWritePerm:        []string{"group12"},
	stableWritePerm:      []string{"group2"},
	channels:             []params.Channel{params.EdgeChannel, params.StableChannel},
}, {
	about:                "access provided through stable channel, but charm not published",
	username:             "kirk",
	groups:               []string{"group1", "group2", "group3"},
	unpublishedWritePerm: []string{"picard", "sisko", "group42", "group47"},
	edgeWritePerm:        []string{"group12"},
	stableWritePerm:      []string{"group2"},
	channels:             []params.Channel{params.EdgeChannel},
	expectStatus:         http.StatusUnauthorized,
	expectBody: params.Error{
		Code:    params.ErrUnauthorized,
		Message: `unauthorized: access denied for user "kirk"`,
	},
}, {
	about:                "access provided through edge channel, but charm on stable channel",
	username:             "kirk",
	groups:               []string{"group1", "group2", "group3"},
	unpublishedWritePerm: []string{"picard", "sisko", "group42", "group47"},
	edgeWritePerm:        []string{"group1"},
	stableWritePerm:      []string{"group11"},
	channels: []params.Channel{
		params.EdgeChannel,
		params.StableChannel,
	},
	expectStatus: http.StatusUnauthorized,
	expectBody: params.Error{
		Code:    params.ErrUnauthorized,
		Message: `unauthorized: access denied for user "kirk"`,
	},
}, {
	about:                "access provided through unpublished ACL, but charm on stable channel",
	username:             "kirk",
	groups:               []string{"group1", "group2", "group3"},
	unpublishedWritePerm: []string{"picard", "sisko", "group42", "group1"},
	stableWritePerm:      []string{"group11"},
	channels: []params.Channel{
		params.EdgeChannel,
		params.StableChannel,
	},
	expectStatus: http.StatusUnauthorized,
	expectBody: params.Error{
		Code:    params.ErrUnauthorized,
		Message: `unauthorized: access denied for user "kirk"`,
	},
}, {
	about:                "access provided through unpublished ACL, but charm on edge channel",
	username:             "kirk",
	groups:               []string{"group1", "group2", "group3"},
	unpublishedWritePerm: []string{"picard", "sisko", "group42", "group1"},
	edgeWritePerm:        []string{"group11"},
	channels: []params.Channel{
		params.EdgeChannel,
	},
	expectStatus: http.StatusUnauthorized,
	expectBody: params.Error{
		Code:    params.ErrUnauthorized,
		Message: `unauthorized: access denied for user "kirk"`,
	},
}}

func (s *authSuite) TestWriteAuthorization(c *gc.C) {
	for i, test := range writeAuthorizationTests {
		c.Logf("test %d: %s", i, test.about)

		s.discharge = dischargeForUser(test.username)
		s.idM.groups = map[string][]string{
			test.username: test.groups,
		}

		// Add a charm to the store, used for testing.
		rurl := newResolvedURL("~charmers/utopic/wordpress-42", -1)
		err := s.store.AddCharmWithArchive(rurl, storetesting.Charms.CharmDir("wordpress"))
		c.Assert(err, gc.IsNil)

		// publish the charm on any required channels.
		if len(test.channels) > 0 {
			err := s.store.Publish(rurl, nil, test.channels...)
			c.Assert(err, gc.IsNil)
		}

		// Change the ACLs for the testing charm.
		err = s.store.SetPerms(&rurl.URL, "unpublished.write", test.unpublishedWritePerm...)
		c.Assert(err, gc.IsNil)
		err = s.store.SetPerms(&rurl.URL, "edge.write", test.edgeWritePerm...)
		c.Assert(err, gc.IsNil)
		err = s.store.SetPerms(&rurl.URL, "stable.write", test.stableWritePerm...)
		c.Assert(err, gc.IsNil)

		makeRequest := func(path string, expectStatus int, expectBody interface{}) {
			client := httpbakery.NewHTTPClient()
			rec := httptesting.DoRequest(c, httptesting.DoRequestParams{
				Handler: s.srv,
				Do:      bakeryDo(client),
				URL:     storeURL(path),
				Method:  "PUT",
				Header:  http.Header{"Content-Type": {"application/json"}},
				Body:    strings.NewReader("42"),
			})
			if expectStatus == 0 {
				expectStatus = http.StatusOK
			}
			c.Assert(rec.Code, gc.Equals, expectStatus, gc.Commentf("body: %s", rec.Body))
			if expectBody != nil {
				c.Assert(rec.Body.String(), jc.JSONEquals, expectBody)
			}
		}

		// Perform a meta PUT request to the URLs.
		// Note that we use the full URL so that we test authorization specifically
		// on that entity without trying to look up the entity in the stable channel.
		makeRequest("~charmers/utopic/wordpress-42/meta/extra-info/key", test.expectStatus, test.expectBody)

		// Remove all entities from the store.
		_, err = s.store.DB.Entities().RemoveAll(nil)
		c.Assert(err, gc.IsNil)
	}
}

var uploadEntityAuthorizationTests = []struct {
	// about holds the test description.
	about string
	// username holds the authenticated user name returned by the discharger.
	// If empty, an anonymous user is returned.
	username string
	// groups holds group names the user is member of, as returned by the
	// discharger.
	groups []string
	// id holds the id of the entity to be uploaded.
	id string
	// promulgated holds whether the corresponding promulgated entity must be
	// already present in the charm store before performing the upload.
	promulgated bool
	// writeAcls can be used to set customized write ACLs for the published
	// entity before performing the upload. If empty, default ACLs are used.
	writeAcls []string
	// expectStatus is the expected HTTP response status.
	// Defaults to 200 status OK.
	expectStatus int
	// expectBody holds the expected body of the HTTP response. If nil,
	// the body is not checked and the response is assumed to be ok.
	expectBody interface{}
}{{
	about:    "user owned entity",
	username: "who",
	id:       "~who/utopic/django",
}, {
	about:    "group owned entity",
	username: "dalek",
	groups:   []string{"group1", "group2"},
	id:       "~group1/utopic/django",
}, {
	about:    "specific group",
	username: "dalek",
	groups:   []string{"group42"},
	id:       "~group42/utopic/django",
}, {
	about:       "promulgated entity",
	username:    "sisko",
	groups:      []string{"charmers", "group2"},
	id:          "~charmers/utopic/django",
	promulgated: true,
}, {
	about:        "unauthorized: promulgated entity",
	username:     "sisko",
	groups:       []string{"group1", "group2"},
	id:           "~charmers/utopic/django",
	promulgated:  true,
	expectStatus: http.StatusUnauthorized,
	expectBody: params.Error{
		Code:    params.ErrUnauthorized,
		Message: `unauthorized: access denied for user "sisko"`,
	},
}, {
	about:        "unauthorized: anonymous user",
	id:           "~who/utopic/django",
	expectStatus: http.StatusUnauthorized,
	expectBody: params.Error{
		Code:    params.ErrUnauthorized,
		Message: "unauthorized: no username declared",
	},
}, {
	about:        "unauthorized: anonymous user and promulgated entity",
	id:           "~charmers/utopic/django",
	promulgated:  true,
	expectStatus: http.StatusUnauthorized,
	expectBody: params.Error{
		Code:    params.ErrUnauthorized,
		Message: "unauthorized: no username declared",
	},
}, {
	about:        "unauthorized: user does not match",
	username:     "kirk",
	id:           "~picard/utopic/django",
	expectStatus: http.StatusUnauthorized,
	expectBody: params.Error{
		Code:    params.ErrUnauthorized,
		Message: `unauthorized: access denied for user "kirk"`,
	},
}, {
	about:        "unauthorized: group does not match",
	username:     "kirk",
	groups:       []string{"group1", "group2", "group3"},
	id:           "~group0/utopic/django",
	expectStatus: http.StatusUnauthorized,
	expectBody: params.Error{
		Code:    params.ErrUnauthorized,
		Message: `unauthorized: access denied for user "kirk"`,
	},
}, {
	about:        "unauthorized: specific group and promulgated entity",
	username:     "janeway",
	groups:       []string{"group1"},
	id:           "~charmers/utopic/django",
	promulgated:  true,
	expectStatus: http.StatusUnauthorized,
	expectBody: params.Error{
		Code:    params.ErrUnauthorized,
		Message: `unauthorized: access denied for user "janeway"`,
	},
}, {
	about:        "unauthorized: entity no permissions",
	username:     "picard",
	id:           "~picard/wily/django",
	writeAcls:    []string{"kirk"},
	expectStatus: http.StatusUnauthorized,
	expectBody: params.Error{
		Code:    params.ErrUnauthorized,
		Message: `unauthorized: access denied for user "picard"`,
	},
}}

func (s *authSuite) TestUploadEntityAuthorization(c *gc.C) {
	for i, test := range uploadEntityAuthorizationTests {
		c.Logf("test %d: %s", i, test.about)

		s.discharge = dischargeForUser(test.username)
		s.idM.groups = map[string][]string{
			test.username: test.groups,
		}

		// Prepare the expected status.
		expectStatus := test.expectStatus
		if expectStatus == 0 {
			expectStatus = http.StatusOK
		}

		// Add a pre-existing entity if required.
		if test.promulgated || len(test.writeAcls) != 0 {
			id := charm.MustParseURL(test.id).WithRevision(0)
			revision := -1
			if test.promulgated {
				revision = 1
			}
			rurl := newResolvedURL(id.String(), revision)
			s.store.AddCharmWithArchive(rurl, storetesting.Charms.CharmArchive(c.MkDir(), "mysql"))
			if len(test.writeAcls) != 0 {
				s.store.SetPerms(&rurl.URL, "unpublished.write", test.writeAcls...)
			}
		}

		// Try to upload the entity.
		body, hash, size := archiveInfo(c, "wordpress")
		defer body.Close()
		client := httpbakery.NewHTTPClient()
		rec := httptesting.DoRequest(c, httptesting.DoRequestParams{
			Handler:       s.srv,
			Do:            bakeryDo(client),
			URL:           storeURL(test.id + "/archive?hash=" + hash),
			Method:        "POST",
			ContentLength: size,
			Header: http.Header{
				"Content-Type": {"application/zip"},
			},
			Body: body,
		})
		c.Assert(rec.Code, gc.Equals, expectStatus, gc.Commentf("body: %s", rec.Body))
		if test.expectBody != nil {
			c.Assert(rec.Body.String(), jc.JSONEquals, test.expectBody)
		}

		// Remove all entities from the store.
		_, err := s.store.DB.Entities().RemoveAll(nil)
		c.Assert(err, gc.IsNil)
		_, err = s.store.DB.BaseEntities().RemoveAll(nil)
		c.Assert(err, gc.IsNil)
	}
}

type readSeekCloser interface {
	io.ReadCloser
	io.Seeker
}

// archiveInfo prepares a zip archive of an entity and return a reader for the
// archive, its blob hash and size.
func archiveInfo(c *gc.C, name string) (r readSeekCloser, hashSum string, size int64) {
	ch := storetesting.Charms.CharmArchive(c.MkDir(), name)
	f, err := os.Open(ch.Path)
	c.Assert(err, gc.IsNil)
	hash, size := hashOf(f)
	_, err = f.Seek(0, 0)
	c.Assert(err, gc.IsNil)
	return f, hash, size
}

var isEntityCaveatTests = []struct {
	url         string
	expectError string
}{{
	url: "~charmers/utopic/wordpress-42/archive",
}, {
	url: "~charmers/utopic/wordpress-42/meta/hash",
}, {
	url: "wordpress/archive",
}, {
	url: "wordpress/meta/hash",
}, {
	url: "utopic/wordpress-10/archive",
}, {
	url: "utopic/wordpress-10/meta/hash",
}, {
	url:         "~charmers/utopic/wordpress-41/archive",
	expectError: `verification failed: caveat "is-entity cs:~charmers/utopic/wordpress-42" not satisfied: operation on entity cs:~charmers/utopic/wordpress-41 not allowed`,
}, {
	url:         "~charmers/utopic/wordpress-41/meta/hash",
	expectError: `verification failed: caveat "is-entity cs:~charmers/utopic/wordpress-42" not satisfied: operation on entity cs:~charmers/utopic/wordpress-41 not allowed`,
}, {
	url:         "utopic/wordpress-9/archive",
	expectError: `verification failed: caveat "is-entity cs:~charmers/utopic/wordpress-42" not satisfied: operation on entity cs:utopic/wordpress-9 not allowed`,
}, {
	url:         "utopic/wordpress-9/meta/hash",
	expectError: `verification failed: caveat "is-entity cs:~charmers/utopic/wordpress-42" not satisfied: operation on entity cs:utopic/wordpress-9 not allowed`,
}, {
	url:         "log",
	expectError: `verification failed: caveat "is-entity cs:~charmers/utopic/wordpress-42" not satisfied: operation does not involve any entities`,
}}

func (s *authSuite) TestIsEntityCaveat(c *gc.C) {
	s.discharge = func(_, _ string) ([]checkers.Caveat, error) {
		return []checkers.Caveat{{
			Condition: "is-entity cs:~charmers/utopic/wordpress-42",
		},
			checkers.DeclaredCaveat(v5.UsernameAttr, "bob"),
		}, nil
	}

	// Add a charm to the store, used for testing.
	s.addPublicCharm(c, storetesting.NewCharm(nil), newResolvedURL("~charmers/utopic/wordpress-41", 9))
	s.addPublicCharm(c, storetesting.NewCharm(nil), newResolvedURL("~charmers/utopic/wordpress-42", 10))
	// Change the ACLs for charms we've just uploaded, otherwise
	// no authorization checking will take place.
	err := s.store.SetPerms(charm.MustParseURL("cs:~charmers/wordpress"), "stable.read", "bob")
	c.Assert(err, gc.IsNil)

	for i, test := range isEntityCaveatTests {
		c.Logf("test %d: %s", i, test.url)
		rec := httptesting.DoRequest(c, httptesting.DoRequestParams{
			Handler: s.srv,
			Do:      bakeryDo(nil),
			URL:     storeURL(test.url),
			Method:  "GET",
		})
		if test.expectError != "" {
			c.Assert(rec.Code, gc.Equals, http.StatusUnauthorized)
			var respErr httpbakery.Error
			err := json.Unmarshal(rec.Body.Bytes(), &respErr)
			c.Assert(err, gc.IsNil)
			c.Assert(respErr.Message, gc.Matches, test.expectError)
			continue
		}
		c.Assert(rec.Code, gc.Equals, http.StatusOK, gc.Commentf("body: %s", rec.Body.Bytes()))
	}
}

func (s *authSuite) TestDelegatableMacaroon(c *gc.C) {
	// Create a new server with a third party discharger.
	s.discharge = dischargeForUser("bob")

	// First check that we get a macaraq error when using a vanilla http do
	// request with both bakery protocol.
	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler: s.srv,
		URL:     storeURL("delegatable-macaroon"),
		Header:  http.Header{"Bakery-Protocol-Version": {"1"}},
		ExpectBody: httptesting.BodyAsserter(func(c *gc.C, m json.RawMessage) {
			// Allow any body - the next check will check that it's a valid macaroon.
		}),
		ExpectStatus: http.StatusUnauthorized,
	})

	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler: s.srv,
		URL:     storeURL("delegatable-macaroon"),
		ExpectBody: httptesting.BodyAsserter(func(c *gc.C, m json.RawMessage) {
			// Allow any body - the next check will check that it's a valid macaroon.
		}),
		ExpectStatus: http.StatusProxyAuthRequired,
	})

	client := httpbakery.NewHTTPClient()

	now := time.Now()
	var gotBody json.RawMessage
	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler: s.srv,
		URL:     storeURL("delegatable-macaroon"),
		ExpectBody: httptesting.BodyAsserter(func(c *gc.C, m json.RawMessage) {
			gotBody = m
		}),
		Do:           bakeryDo(client),
		ExpectStatus: http.StatusOK,
	})

	c.Assert(gotBody, gc.NotNil)
	var m macaroon.Macaroon
	err := json.Unmarshal(gotBody, &m)
	c.Assert(err, gc.IsNil)

	caveats := m.Caveats()
	foundExpiry := false
	for _, cav := range caveats {
		cond, arg, err := checkers.ParseCaveat(cav.Id)
		c.Assert(err, gc.IsNil)
		switch cond {
		case checkers.CondTimeBefore:
			t, err := time.Parse(time.RFC3339Nano, arg)
			c.Assert(err, gc.IsNil)
			c.Assert(t, jc.TimeBetween(now.Add(v5.DelegatableMacaroonExpiry), now.Add(v5.DelegatableMacaroonExpiry+time.Second)))
			foundExpiry = true
		}
	}
	c.Assert(foundExpiry, jc.IsTrue)

	// Now check that we can use the obtained macaroon to do stuff
	// as the declared user.

	rurl := newResolvedURL("~charmers/utopic/wordpress-41", 9)
	err = s.store.AddCharmWithArchive(
		rurl,
		storetesting.Charms.CharmDir("wordpress"))
	c.Assert(err, gc.IsNil)
	err = s.store.Publish(rurl, nil, params.StableChannel)
	c.Assert(err, gc.IsNil)
	// Change the ACLs for the testing charm.
	err = s.store.SetPerms(charm.MustParseURL("cs:~charmers/wordpress"), "stable.read", "bob")
	c.Assert(err, gc.IsNil)

	// First check that we require authorization to access the charm.
	rec := httptesting.DoRequest(c, httptesting.DoRequestParams{
		Handler: s.srv,
		URL:     storeURL("~charmers/utopic/wordpress/meta/id-name"),
		Method:  "GET",
	})
	c.Assert(rec.Code, gc.Equals, http.StatusProxyAuthRequired)

	// Then check that the request succeeds if we provide the delegatable
	// macaroon.

	client = httpbakery.NewHTTPClient()
	u, err := url.Parse("http://127.0.0.1")
	c.Assert(err, gc.IsNil)
	err = httpbakery.SetCookie(client.Jar, u, macaroon.Slice{&m})
	c.Assert(err, gc.IsNil)

	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler: s.srv,
		URL:     storeURL("~charmers/utopic/wordpress/meta/id-name"),
		ExpectBody: params.IdNameResponse{
			Name: "wordpress",
		},

		ExpectStatus: http.StatusOK,
		Do:           bakeryDo(client),
	})
}

func (s *authSuite) TestDelegatableMacaroonWithBasicAuth(c *gc.C) {
	// First check that we get a macaraq error when using a vanilla http do
	// request.
	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler:  s.srv,
		Username: testUsername,
		Password: testPassword,
		URL:      storeURL("delegatable-macaroon"),
		ExpectBody: params.Error{
			Code:    params.ErrForbidden,
			Message: "delegatable macaroon is not obtainable using admin credentials",
		},
		ExpectStatus: http.StatusForbidden,
	})
}

func (s *authSuite) TestDelegatableMacaroonWithIds(c *gc.C) {
	err := s.store.AddCharmWithArchive(newResolvedURL("~charmers/utopic/wordpress-1", 1), storetesting.NewCharm(nil))
	c.Assert(err, gc.IsNil)
	err = s.store.AddCharmWithArchive(newResolvedURL("~charmers/utopic/wordpress-2", 2), storetesting.NewCharm(nil))
	c.Assert(err, gc.IsNil)
	err = s.store.AddCharmWithArchive(newResolvedURL("~charmers/precise/mysql-1", 1), storetesting.NewCharm(nil))
	c.Assert(err, gc.IsNil)

	// Check that we can't acquire a delegatable macaroon if we
	// don't already have permission.
	s.doAsUser("bob", func() {
		httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
			Handler:      s.srv,
			URL:          storeURL("delegatable-macaroon?channel=unpublished&id=wordpress"),
			Do:           bakeryDo(nil),
			ExpectStatus: http.StatusUnauthorized,
			ExpectBody: params.Error{
				Message: `unauthorized: access denied for user "bob"`,
				Code:    params.ErrUnauthorized,
			},
		})
	})

	// Get a delegatable macaroon for "wordpress", which should
	// tied to the latest revision.
	var m *macaroon.Macaroon
	s.doAsUser("charmers", func() {
		m = s.getDelegatableMacaroon(c, params.UnpublishedChannel, "wordpress")
	})

	s.doAsUser("bob", func() {
		httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
			Handler:      s.srv,
			URL:          storeURL("wordpress/meta/id-revision?channel=unpublished"),
			Header:       macaroonHeader(nil, macaroon.Slice{m}),
			ExpectStatus: http.StatusOK,
			ExpectBody: params.IdRevisionResponse{
				Revision: 2,
			},
		})
		// Check that we can't use the macaroon to access a different revision
		// from the original.
		httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
			Handler:      s.srv,
			URL:          storeURL("utopic/wordpress-1/meta/id-revision?channel=unpublished"),
			Do:           bakeryDo(nil),
			Header:       macaroonHeader(nil, macaroon.Slice{m}),
			ExpectStatus: http.StatusUnauthorized,
			ExpectBody: params.Error{
				Message: `unauthorized: access denied for user "bob"`,
				Code:    params.ErrUnauthorized,
			},
		})

		// ... and that we can't use it to access a different charm.
		s.discharge = dischargeForUser("bob")
		httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
			Handler:      s.srv,
			URL:          storeURL("precise/mysql/meta/id-revision?channel=unpublished"),
			Do:           bakeryDo(nil),
			Header:       macaroonHeader(nil, macaroon.Slice{m}),
			ExpectStatus: http.StatusUnauthorized,
			ExpectBody: params.Error{
				Message: `unauthorized: access denied for user "bob"`,
				Code:    params.ErrUnauthorized,
			},
		})
	})
}

func (s *authSuite) TestRenewDelegatableMacaroonWithIds(c *gc.C) {
	err := s.store.AddCharmWithArchive(newResolvedURL("~charmers/utopic/wordpress-1", 1), storetesting.NewCharm(nil))
	c.Assert(err, gc.IsNil)

	// Fake the current time so that we can easily test expiration times.
	t0 := time.Now()
	currentTime := t0
	s.PatchValue(v5.TimeNow, func() time.Time {
		return currentTime
	})

	var m *macaroon.Macaroon
	s.doAsUser("charmers", func() {
		m = s.getDelegatableMacaroon(c, params.UnpublishedChannel, "wordpress")
	})

	// Check that the delegatable macaroon works.
	s.doAsUser("", func() {
		httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
			Handler:      s.srv,
			URL:          storeURL("wordpress/meta/id-revision?channel=unpublished"),
			Header:       macaroonHeader(nil, macaroon.Slice{m}),
			ExpectStatus: http.StatusOK,
			ExpectBody: params.IdRevisionResponse{
				Revision: 1,
			},
		})
	})

	// If we try again after the active expiry time, we should get
	// a discharge-required error.
	currentTime = t0.Add(v5.DelegatableMacaroonExpiry + time.Second)

	s.doAsUser("", func() {
		httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
			Handler:      s.srv,
			URL:          storeURL("wordpress/meta/id-revision?channel=unpublished"),
			Header:       macaroonHeader(nil, macaroon.Slice{m}),
			ExpectStatus: http.StatusProxyAuthRequired,
			ExpectBody: httptesting.BodyAsserter(func(c *gc.C, m json.RawMessage) {
			}),
		})
	})

	// Try with the bakery client - the macaroon should be automatically renewed
	// and stored in the cookie jar.
	client := httpbakery.NewHTTPClient()
	s.doAsUser("", func() {
		httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
			Handler: s.srv,
			Do:      bakeryDo(client),
			URL:     storeURL("wordpress/meta/id-revision?channel=unpublished"),
			Header:  macaroonHeader(nil, macaroon.Slice{m}),
			ExpectBody: params.IdRevisionResponse{
				Revision: 1,
			},
		})
	})

	// Try again without the original macaroon to check that we've
	// actually acquired the renewed macaroon.
	s.doAsUser("", func() {
		httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
			Handler: s.srv,
			Do:      bakeryDo(client),
			URL:     storeURL("wordpress/meta/id-revision?channel=unpublished"),
			ExpectBody: params.IdRevisionResponse{
				Revision: 1,
			},
		})
	})
	// Try again after the second expiry time, using the same cookie
	// jar but not using the bakery client. We should get a discharge-denied error.
	currentTime = currentTime.Add(v5.DelegatableMacaroonExpiry + time.Second)

	client1 := httpbakery.NewHTTPClient()
	client1.Jar = client.Jar
	s.doAsUser("", func() {
		httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
			Handler:      s.srv,
			Do:           client1.Do,
			URL:          storeURL("wordpress/meta/id-revision?channel=unpublished"),
			ExpectStatus: http.StatusProxyAuthRequired,
			ExpectBody: httptesting.BodyAsserter(func(c *gc.C, m json.RawMessage) {
			}),
		})
	})
	// Check again that we can renew the macaroon.
	s.doAsUser("", func() {
		httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
			Handler: s.srv,
			Do:      bakeryDo(client),
			URL:     storeURL("wordpress/meta/id-revision?channel=unpublished"),
			ExpectBody: params.IdRevisionResponse{
				Revision: 1,
			},
		})
	})
}

func (s *authSuite) TestDelegatableMacaroonCannotBeUsedForWriting(c *gc.C) {
	currentTime := time.Now()
	s.PatchValue(v5.TimeNow, func() time.Time {
		return currentTime
	})

	id := newResolvedURL("~charmers/utopic/wordpress-1", 1)
	err := s.store.AddCharmWithArchive(id, storetesting.NewCharm(nil))
	c.Assert(err, gc.IsNil)
	err = s.store.SetPerms(&id.URL, "stable.read", "bob")
	c.Assert(err, gc.IsNil)
	err = s.store.Publish(id, nil, params.StableChannel)
	c.Assert(err, gc.IsNil)

	var m *macaroon.Macaroon
	s.doAsUser("bob", func() {
		m = s.getDelegatableMacaroon(c, params.StableChannel, "~charmers/utopic/wordpress-1")
	})

	s.doAsUser("", func() {
		httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
			Handler:      s.srv,
			Method:       "PUT",
			URL:          storeURL("wordpress/meta/extra-info/foo"),
			Header:       macaroonHeader(nil, macaroon.Slice{m}),
			ExpectStatus: http.StatusUnauthorized,
			ExpectBody: params.Error{
				Code:    params.ErrUnauthorized,
				Message: `unauthorized: access denied for user "bob"`,
			},
		})
	})
}

func (s *authSuite) TestRenewMacaroon(c *gc.C) {
	m, err := macaroon.New([]byte("key"), "id", "somewhere")
	c.Assert(err, gc.IsNil)
	m.AddFirstPartyCaveat("active-time-before xxx")
	m.AddFirstPartyCaveat("hello")
	m.AddThirdPartyCaveat([]byte("otherkey"), "x", "location")
	m.AddFirstPartyCaveat("goodbye")
	m.AddFirstPartyCaveat("active-time-before yyy")

	newm, err := macaroon.New([]byte("otherkey"), "id2", "somewhere")
	c.Assert(err, gc.IsNil)

	expectTime := "2016-04-22T16:30:00Z"

	expiry, err := time.Parse(time.RFC3339, expectTime)
	c.Assert(err, gc.IsNil)

	err = v5.RenewMacaroon(newm, macaroon.Slice{m}, expiry)
	c.Assert(err, gc.IsNil)

	// The new macaroon should have all the first party caveats
	// from the original, omitting the third party caveats
	// and with a new active-time-before caveat.
	c.Assert(newm.Caveats(), jc.DeepEquals, []macaroon.Caveat{{
		Id: "hello",
	}, {
		Id: "goodbye",
	}, {
		Id: "active-time-before " + expectTime,
	}})
}

func (s *authSuite) TestNewServerWithIdentityLocationButNotIdentityAPIURL(c *gc.C) {
	// If the IdentityAPIURL is empty but IdentityLocation is not, then
	// we can discharge but not get groups.
	config := s.srvParams
	config.IdentityAPIURL = ""

	db := s.Session.DB("charmstore")
	srv, err := charmstore.NewServer(db, nil, config, map[string]charmstore.NewAPIHandlerFunc{"v5": v5.NewAPIHandler})
	c.Assert(err, gc.IsNil)
	defer srv.Close()

	s.discharge = dischargeForUser("bob")
	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler: srv,
		URL:     storeURL("whoami"),
		Do:      bakeryDo(nil),
		ExpectBody: params.WhoAmIResponse{
			User: "bob",
		},
	})
}

// getDelegatableMacaroon acquires a delegatable macaroon good for
// accessing the given URLs.
func (s *authSuite) getDelegatableMacaroon(c *gc.C, ch params.Channel, urlStrs ...string) *macaroon.Macaroon {
	var gotBody json.RawMessage
	p := url.Values{
		"id": urlStrs,
	}
	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler: s.srv,
		URL:     storeURL("delegatable-macaroon?channel=" + string(ch) + "&" + p.Encode()),
		Do:      bakeryDo(nil),
		ExpectBody: httptesting.BodyAsserter(func(c *gc.C, body json.RawMessage) {
			gotBody = body
		}),
	})
	c.Assert(gotBody, gc.NotNil)
	var m macaroon.Macaroon
	err := json.Unmarshal(gotBody, &m)
	c.Assert(err, jc.ErrorIsNil)
	return &m
}

type errorTransport string

func (e errorTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errgo.New(string(e))
}

func dischargeForUser(username string) func(_, _ string) ([]checkers.Caveat, error) {
	return func(_, _ string) ([]checkers.Caveat, error) {
		return []checkers.Caveat{
			checkers.DeclaredCaveat(v5.UsernameAttr, username),
		}, nil
	}
}

func noDischarge(_, _ string) ([]checkers.Caveat, error) {
	return nil, errgo.New("no discharge")
}

func macaroonHeader(h http.Header, ms macaroon.Slice) http.Header {
	if h == nil {
		h = make(http.Header)
	}
	data, err := json.Marshal(ms)
	if err != nil {
		panic(err)
	}
	value := base64.StdEncoding.EncodeToString(data)
	h.Add(httpbakery.MacaroonsHeader, value)
	return h
}
