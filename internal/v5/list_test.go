// Copyright 2014 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package v5_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/juju/idmclient"
	"github.com/juju/loggo"
	jc "github.com/juju/testing/checkers"
	"github.com/juju/testing/httptesting"
	gc "gopkg.in/check.v1"
	"gopkg.in/juju/charm.v6"
	"gopkg.in/juju/charmrepo.v2/csclient/params"
	"gopkg.in/macaroon-bakery.v2-unstable/bakery/checkers"
	"gopkg.in/macaroon-bakery.v2-unstable/httpbakery"
	"gopkg.in/macaroon.v2-unstable"
	"gopkg.in/mgo.v2/bson"

	"gopkg.in/juju/charmstore.v5/internal/charmstore"
	"gopkg.in/juju/charmstore.v5/internal/router"
	"gopkg.in/juju/charmstore.v5/internal/storetesting"
)

type ListSuite struct {
	commonSuite
}

var _ = gc.Suite(&ListSuite{})

var exportListTestCharms = map[string]*router.ResolvedURL{
	"wordpress": newResolvedURL("cs:~charmers/precise/wordpress-23", 23),
	"mysql":     newResolvedURL("cs:~openstack-charmers/trusty/mysql-7", 7),
	"varnish":   newResolvedURL("cs:~foo/trusty/varnish-1", -1),
	"riak":      newResolvedURL("cs:~charmers/trusty/riak-67", 67),
}

var exportListTestBundles = map[string]*router.ResolvedURL{
	"wordpress-simple": newResolvedURL("cs:~charmers/bundle/wordpress-simple-4", 4),
}

func (s *ListSuite) SetUpSuite(c *gc.C) {
	s.enableIdentity = true
	s.commonSuite.SetUpSuite(c)
}

func (s *ListSuite) SetUpTest(c *gc.C) {
	s.commonSuite.SetUpTest(c)
}

func (s *ListSuite) addCharmsToStore(c *gc.C) {
	for name, id := range exportListTestCharms {
		s.addPublicCharm(c, getListCharm(name), id)
	}
	for name, id := range exportListTestBundles {
		s.addPublicBundle(c, getListBundle(name), id, false)
	}
	// hide the riak charm
	err := s.store.SetPerms(charm.MustParseURL("cs:~charmers/riak"), "stable.read", "charmers", "test-user")
	c.Assert(err, gc.Equals, nil)
}

func getListCharm(name string) *storetesting.Charm {
	ca := storetesting.Charms.CharmDir(name)
	meta := ca.Meta()
	meta.Categories = append(strings.Split(name, "-"), "bar")
	return storetesting.NewCharm(meta)
}

func getListBundle(name string) *storetesting.Bundle {
	ba := storetesting.Charms.BundleDir(name)
	data := ba.Data()
	data.Tags = append(strings.Split(name, "-"), "baz")
	return storetesting.NewBundle(data)
}

func (s *ListSuite) TestSuccessfulList(c *gc.C) {
	tests := []struct {
		about   string
		query   string
		results []string
	}{{
		about: "bare list",
		query: "",
		results: []string{
			"cs:bundle/wordpress-simple-4",
			"cs:precise/wordpress-23",
			"cs:trusty/mysql-7",
			"cs:~foo/trusty/varnish-1",
		},
	}, {
		about: "name filter list",
		query: "name=mysql",
		results: []string{
			"cs:trusty/mysql-7",
		},
	}, {
		about: "owner filter list",
		query: "owner=foo",
		results: []string{
			"cs:~foo/trusty/varnish-1",
		},
	}, {
		about: "series filter list",
		query: "series=trusty",
		results: []string{
			"cs:trusty/mysql-7",
			"cs:~foo/trusty/varnish-1",
		},
	}, {
		about: "type filter list",
		query: "type=bundle",
		results: []string{
			"cs:bundle/wordpress-simple-4",
		},
	}, {
		about: "promulgated",
		query: "promulgated=1",
		results: []string{
			"cs:bundle/wordpress-simple-4",
			"cs:precise/wordpress-23",
			"cs:trusty/mysql-7",
		},
	}, {
		about: "not promulgated",
		query: "promulgated=0",
		results: []string{
			"cs:~foo/trusty/varnish-1",
		},
	}, {
		about: "promulgated with owner",
		query: "promulgated=1&owner=openstack-charmers",
		results: []string{
			"cs:trusty/mysql-7",
		},
	}}
	s.addCharmsToStore(c)
	for i, test := range tests {
		c.Logf("test %d. %s", i, test.about)
		rec := httptesting.DoRequest(c, httptesting.DoRequestParams{
			Handler: s.srv,
			URL:     storeURL("list?" + test.query),
		})
		var sr params.ListResponse
		err := json.Unmarshal(rec.Body.Bytes(), &sr)
		c.Assert(err, gc.Equals, nil)
		assertListResult(c, sr, test.results)
	}
}

func (s *ListSuite) TestMetadataFields(c *gc.C) {
	tests := []struct {
		about string
		query string
		meta  map[string]interface{}
	}{{
		about: "archive-size",
		query: "name=mysql&include=archive-size",
		meta: map[string]interface{}{
			"archive-size": params.ArchiveSizeResponse{getListCharm("mysql").Size()},
		},
	}, {
		about: "bundle-metadata",
		query: "name=wordpress-simple&type=bundle&include=bundle-metadata",
		meta: map[string]interface{}{
			"bundle-metadata": getListBundle("wordpress-simple").Data(),
		},
	}, {
		about: "bundle-machine-count",
		query: "name=wordpress-simple&type=bundle&include=bundle-machine-count",
		meta: map[string]interface{}{
			"bundle-machine-count": params.BundleCount{2},
		},
	}, {
		about: "bundle-unit-count",
		query: "name=wordpress-simple&type=bundle&include=bundle-unit-count",
		meta: map[string]interface{}{
			"bundle-unit-count": params.BundleCount{2},
		},
	}, {
		about: "charm-actions",
		query: "name=wordpress&type=charm&include=charm-actions",
		meta: map[string]interface{}{
			"charm-actions": getListCharm("wordpress").Actions(),
		},
	}, {
		about: "charm-config",
		query: "name=wordpress&type=charm&include=charm-config",
		meta: map[string]interface{}{
			"charm-config": getListCharm("wordpress").Config(),
		},
	}, {
		about: "charm-related",
		query: "name=wordpress&type=charm&include=charm-related",
		meta: map[string]interface{}{
			"charm-related": params.RelatedResponse{
				Provides: map[string][]params.EntityResult{
					"mysql": {
						{
							Id: charm.MustParseURL("cs:trusty/mysql-7"),
						},
					},
					"varnish": {
						{
							Id: charm.MustParseURL("cs:~foo/trusty/varnish-1"),
						},
					},
				},
			},
		},
	}, {
		about: "multiple values",
		query: "name=wordpress&type=charm&include=charm-related&include=charm-config",
		meta: map[string]interface{}{
			"charm-related": params.RelatedResponse{
				Provides: map[string][]params.EntityResult{
					"mysql": {
						{
							Id: charm.MustParseURL("cs:trusty/mysql-7"),
						},
					},
					"varnish": {
						{
							Id: charm.MustParseURL("cs:~foo/trusty/varnish-1"),
						},
					},
				},
			},
			"charm-config": getListCharm("wordpress").Config(),
		},
	}}
	s.addCharmsToStore(c)
	for i, test := range tests {
		c.Logf("test %d. %s", i, test.about)
		rec := httptesting.DoRequest(c, httptesting.DoRequestParams{
			Handler: s.srv,
			URL:     storeURL("list?" + test.query),
		})
		c.Assert(rec.Code, gc.Equals, http.StatusOK)
		var sr struct {
			Results []struct {
				Meta json.RawMessage
			}
		}
		err := json.Unmarshal(rec.Body.Bytes(), &sr)
		c.Assert(err, gc.Equals, nil)
		c.Assert(sr.Results, gc.HasLen, 1)
		c.Assert(string(sr.Results[0].Meta), jc.JSONEquals, test.meta)
	}
}

func (s *ListSuite) TestListIncludeError(c *gc.C) {
	s.addCharmsToStore(c)
	// Perform a list for all charms, including the
	// manifest, which will try to retrieve all charm
	// blobs.
	rec := httptesting.DoRequest(c, httptesting.DoRequestParams{
		Handler: s.srv,
		URL:     storeURL("list?type=charm&include=manifest"),
	})
	c.Assert(rec.Code, gc.Equals, http.StatusOK)
	var resp params.ListResponse
	err := json.Unmarshal(rec.Body.Bytes(), &resp)
	// cs:riak will not be found because it is not visible to "everyone".
	c.Assert(resp.Results, gc.HasLen, len(exportListTestCharms)-1)

	// Now update the entity to hold an invalid hash.
	// The list should still work, but only return a single result.
	err = s.store.UpdateEntity(newResolvedURL("~charmers/precise/wordpress-23", 23), bson.D{{
		"$set", bson.D{{
			"blobhash", hashOfString("nope"),
		}},
	}})
	c.Assert(err, gc.Equals, nil)

	// Now list again - we should get one result less
	// (and the error will be logged).

	// Register a logger that so that we can check the logging output.
	// It will be automatically removed later because IsolatedMgoESSuite
	// uses LoggingSuite.
	var tw loggo.TestWriter
	err = loggo.RegisterWriter("test-log", &tw)
	c.Assert(err, gc.Equals, nil)

	rec = httptesting.DoRequest(c, httptesting.DoRequestParams{
		Handler: s.srv,
		URL:     storeURL("list?type=charm&include=manifest"),
	})
	c.Assert(rec.Code, gc.Equals, http.StatusOK)
	resp = params.ListResponse{}
	err = json.Unmarshal(rec.Body.Bytes(), &resp)
	// cs:riak will not be found because it is not visible to "everyone".
	// cs:wordpress will not be found because it has no manifest.
	c.Assert(resp.Results, gc.HasLen, len(exportListTestCharms)-2)

	c.Assert(tw.Log(), jc.LogMatches, []string{"cannot retrieve metadata for cs:precise/wordpress-23: cannot open archive data for cs:precise/wordpress-23: .*"})
}

func (s *ListSuite) TestSortingList(c *gc.C) {
	tests := []struct {
		about   string
		query   string
		results []string
	}{{
		about: "name ascending",
		query: "sort=name",
		results: []string{
			"cs:trusty/mysql-7",
			"cs:~foo/trusty/varnish-1",
			"cs:precise/wordpress-23",
			"cs:bundle/wordpress-simple-4",
		},
	}, {
		about: "name descending",
		query: "sort=-name",
		results: []string{
			"cs:bundle/wordpress-simple-4",
			"cs:precise/wordpress-23",
			"cs:~foo/trusty/varnish-1",
			"cs:trusty/mysql-7",
		},
	}, {
		about: "series ascending",
		query: "sort=series,name",
		results: []string{
			"cs:bundle/wordpress-simple-4",
			"cs:precise/wordpress-23",
			"cs:trusty/mysql-7",
			"cs:~foo/trusty/varnish-1",
		},
	}, {
		about: "series descending",
		query: "sort=-series&sort=name",
		results: []string{
			"cs:trusty/mysql-7",
			"cs:~foo/trusty/varnish-1",
			"cs:precise/wordpress-23",
			"cs:bundle/wordpress-simple-4",
		},
	}, {
		about: "owner ascending",
		query: "sort=owner,name",
		results: []string{
			"cs:trusty/mysql-7",
			"cs:precise/wordpress-23",
			"cs:bundle/wordpress-simple-4",
			"cs:~foo/trusty/varnish-1",
		},
	}, {
		about: "owner descending",
		query: "sort=-owner&sort=name",
		results: []string{
			"cs:~foo/trusty/varnish-1",
			"cs:trusty/mysql-7",
			"cs:precise/wordpress-23",
			"cs:bundle/wordpress-simple-4",
		},
	}}
	s.addCharmsToStore(c)
	for i, test := range tests {
		c.Logf("test %d. %s", i, test.about)
		rec := httptesting.DoRequest(c, httptesting.DoRequestParams{
			Handler: s.srv,
			URL:     storeURL("list?" + test.query),
		})
		var sr params.ListResponse
		err := json.Unmarshal(rec.Body.Bytes(), &sr)
		c.Assert(err, gc.Equals, nil)
		c.Assert(sr.Results, gc.HasLen, len(test.results), gc.Commentf("expected %#v", test.results))
		c.Logf("results: %s", rec.Body.Bytes())
		assertListResult(c, sr, test.results)
	}
}

func (s *ListSuite) TestSortUnsupportedListField(c *gc.C) {
	rec := httptesting.DoRequest(c, httptesting.DoRequestParams{
		Handler: s.srv,
		URL:     storeURL("list?sort=text"),
	})
	var e params.Error
	err := json.Unmarshal(rec.Body.Bytes(), &e)
	c.Assert(err, gc.Equals, nil)
	c.Assert(e.Code, gc.Equals, params.ErrBadRequest)
	c.Assert(e.Message, gc.Equals, "invalid sort field: unrecognized sort parameter \"text\"")
}

func (s *ListSuite) TestGetLatestRevisionOnly(c *gc.C) {
	s.addCharmsToStore(c)
	id := newResolvedURL("cs:~charmers/precise/wordpress-24", 24)
	s.addPublicCharm(c, getListCharm("wordpress"), id)

	wantResults := []string{
		"cs:bundle/wordpress-simple-4",
		"cs:precise/wordpress-24",
		"cs:trusty/mysql-7",
		"cs:~foo/trusty/varnish-1",
	}

	rec := httptesting.DoRequest(c, httptesting.DoRequestParams{
		Handler: s.srv,
		URL:     storeURL("list"),
	})
	var sr params.ListResponse
	err := json.Unmarshal(rec.Body.Bytes(), &sr)
	c.Assert(err, gc.Equals, nil)
	assertListResult(c, sr, wantResults)

	wantResults = []string{
		"cs:trusty/mysql-7",
		"cs:~foo/trusty/varnish-1",
		"cs:precise/wordpress-24",
		"cs:bundle/wordpress-simple-4",
	}
	rec = httptesting.DoRequest(c, httptesting.DoRequestParams{
		Handler: s.srv,
		URL:     storeURL("list?sort=name"),
	})
	err = json.Unmarshal(rec.Body.Bytes(), &sr)
	c.Assert(err, gc.Equals, nil)
	assertListResult(c, sr, wantResults)
}

func (s *ListSuite) assertPut(c *gc.C, url string, val interface{}) {
	body, err := json.Marshal(val)
	c.Assert(err, gc.Equals, nil)
	rec := httptesting.DoRequest(c, httptesting.DoRequestParams{
		Handler: s.srv,
		URL:     storeURL(url),
		Method:  "PUT",
		Header: http.Header{
			"Content-Type": {"application/json"},
		},
		Username: testUsername,
		Password: testPassword,
		Body:     bytes.NewReader(body),
	})
	c.Assert(rec.Code, gc.Equals, http.StatusOK, gc.Commentf("headers: %v, body: %s", rec.HeaderMap, rec.Body.String()))
	c.Assert(rec.Body.String(), gc.HasLen, 0)
}

func (s *ListSuite) TestListWithAdminCredentials(c *gc.C) {
	s.addCharmsToStore(c)

	rec := httptesting.DoRequest(c, httptesting.DoRequestParams{
		Handler:  s.srv,
		URL:      storeURL("list"),
		Username: testUsername,
		Password: testPassword,
	})
	c.Assert(rec.Code, gc.Equals, http.StatusOK)
	expected := []string{
		"cs:bundle/wordpress-simple-4",
		"cs:precise/wordpress-23",
		"cs:trusty/mysql-7",
		"cs:trusty/riak-67",
		"cs:~foo/trusty/varnish-1",
	}
	var sr params.ListResponse
	err := json.Unmarshal(rec.Body.Bytes(), &sr)
	c.Assert(err, gc.Equals, nil)
	assertListResult(c, sr, expected)
}

func (s *ListSuite) TestListWithUserMacaroon(c *gc.C) {
	s.addCharmsToStore(c)
	rec := httptesting.DoRequest(c, httptesting.DoRequestParams{
		Handler: s.srv,
		URL:     storeURL("list"),
		Do:      bakeryDo(s.login("test-user")),
	})
	c.Assert(rec.Code, gc.Equals, http.StatusOK)
	expected := []string{
		"cs:bundle/wordpress-simple-4",
		"cs:precise/wordpress-23",
		"cs:trusty/mysql-7",
		"cs:trusty/riak-67",
		"cs:~foo/trusty/varnish-1",
	}
	var sr params.ListResponse
	err := json.Unmarshal(rec.Body.Bytes(), &sr)
	c.Assert(err, gc.Equals, nil)
	for i, r := range sr.Results {
		c.Logf("result %d: %v", i, r.Id)
	}
	assertListResult(c, sr, expected)
}

type listWithChannelsTest struct {
	about         string
	charms        []listCharm
	perms         []charmPerms
	expectResults []string
}

type listCharm struct {
	id       string
	channels []params.Channel
}

type charmPerms struct {
	url            string
	publicChannels []params.Channel
}

func (t listWithChannelsTest) test(c *gc.C, store *charmstore.Store, srv *charmstore.Server) {
	// Remove all entities from the store so that we start with a clean slate.
	_, err := store.DB.Entities().RemoveAll(nil)
	c.Assert(err, gc.Equals, nil)
	_, err = store.DB.BaseEntities().RemoveAll(nil)
	c.Assert(err, gc.Equals, nil)

	for _, charmSpec := range t.charms {
		id := newResolvedURL(charmSpec.id, -1)
		meta := new(charm.Meta)
		if id.URL.Series == "" {
			meta.Series = []string{"quantal"}
		}
		ch := storetesting.NewCharm(meta)
		err := store.AddCharmWithArchive(id, ch)
		c.Assert(err, gc.Equals, nil)
		if len(charmSpec.channels) > 0 {
			err = store.Publish(id, nil, charmSpec.channels...)
			c.Assert(err, gc.Equals, nil)
		}
	}
	for _, permSpec := range t.perms {
		url := charm.MustParseURL(permSpec.url)
		for _, channel := range permSpec.publicChannels {
			err := store.SetPerms(url, string(channel)+".read", params.Everyone)
			c.Assert(err, gc.Equals, nil)
		}
	}
	rec := httptesting.DoRequest(c, httptesting.DoRequestParams{
		Handler: srv,
		URL:     storeURL("list"),
	})
	c.Assert(rec.Code, gc.Equals, http.StatusOK)
	var sr params.ListResponse
	err = json.Unmarshal(rec.Body.Bytes(), &sr)
	c.Assert(err, gc.Equals, nil)
	for i, r := range sr.Results {
		c.Logf("result %d: %v", i, r.Id)
	}
	assertListResult(c, sr, t.expectResults)
}

var listWithChannelsTests = []listWithChannelsTest{{
	about: "a later unpublished revision should not override an earlier published revision",
	charms: []listCharm{{
		id:       "cs:~bob/foo-1",
		channels: []params.Channel{params.StableChannel},
	}, {
		id: "cs:~bob/foo-2",
	}},
	perms: []charmPerms{{
		url:            "cs:~bob/foo",
		publicChannels: []params.Channel{params.StableChannel},
	}},
	expectResults: []string{"cs:~bob/foo-1"},
}, {
	about: "only the latest revision should be returned",
	charms: []listCharm{{
		id:       "cs:~bob/foo-1",
		channels: []params.Channel{params.StableChannel},
	}, {
		id:       "cs:~bob/foo-2",
		channels: []params.Channel{params.CandidateChannel},
	}},
	perms: []charmPerms{{
		url:            "cs:~bob/foo",
		publicChannels: []params.Channel{params.CandidateChannel, params.StableChannel},
	}},
	expectResults: []string{"cs:~bob/foo-2"},
}, {
	about: "sort by series",
	charms: []listCharm{{
		id:       "cs:~bob/quantal/foo-1",
		channels: []params.Channel{params.StableChannel},
	}, {
		id:       "cs:~bob/precise/foo-2",
		channels: []params.Channel{params.CandidateChannel},
	}, {
		id:       "cs:~bob/xenial/foo-3",
		channels: []params.Channel{params.EdgeChannel},
	}, {
		id:       "cs:~bob/xenial/foo-4",
		channels: []params.Channel{params.EdgeChannel},
	}},
	perms: []charmPerms{{
		url:            "cs:~bob/foo",
		publicChannels: []params.Channel{params.EdgeChannel, params.CandidateChannel, params.StableChannel},
	}},
	expectResults: []string{"cs:~bob/precise/foo-2", "cs:~bob/quantal/foo-1", "cs:~bob/xenial/foo-4"},
}}

func (s *ListSuite) TestListWithChannels(c *gc.C) {
	for i, test := range listWithChannelsTests {
		c.Logf("%d: %s", i, test.about)
		test.test(c, s.store, s.srv)
	}
}

func (s *ListSuite) TestSearchWithBadAdminCredentialsAndACookie(c *gc.C) {
	s.addCharmsToStore(c)
	m, err := s.store.Bakery.NewMacaroon([]checkers.Caveat{
		idmclient.UserDeclaration("test-user"),
	})
	c.Assert(err, gc.Equals, nil)
	macaroonCookie, err := httpbakery.NewCookie(macaroon.Slice{m})
	c.Assert(err, gc.Equals, nil)
	rec := httptesting.DoRequest(c, httptesting.DoRequestParams{
		Handler:  s.srv,
		URL:      storeURL("list"),
		Cookies:  []*http.Cookie{macaroonCookie},
		Username: testUsername,
		Password: "bad-password",
	})
	c.Assert(rec.Code, gc.Equals, http.StatusOK)
	expected := []string{
		"cs:bundle/wordpress-simple-4",
		"cs:precise/wordpress-23",
		"cs:trusty/mysql-7",
		"cs:~foo/trusty/varnish-1",
	}
	var sr params.ListResponse
	err = json.Unmarshal(rec.Body.Bytes(), &sr)
	c.Assert(err, gc.Equals, nil)
	assertListResult(c, sr, expected)
}

func assertListResult(c *gc.C, got params.ListResponse, want []string) {
	gotStrs := make([]string, len(got.Results))
	for i, r := range got.Results {
		gotStrs[i] = r.Id.String()
	}
	c.Assert(gotStrs, jc.DeepEquals, want)
}
