// Copyright 2012 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package v4_test

import (
	"net/http"
	"time"

	"github.com/juju/charm/v7"
	"github.com/juju/charmrepo/v5/csclient/params"
	"github.com/juju/testing/httptesting"
	gc "gopkg.in/check.v1"

	"gopkg.in/juju/charmstore.v5/internal/charmstore"
	"gopkg.in/juju/charmstore.v5/internal/storetesting"
	v5 "gopkg.in/juju/charmstore.v5/internal/v5"
)

type StatsSuite struct {
	commonSuite
}

var _ = gc.Suite(&StatsSuite{})

func (s *StatsSuite) TestServerStatsUpdate(c *gc.C) {
	ref := charm.MustParseURL("~charmers/precise/wordpress-23")
	tests := []struct {
		path          string
		status        int
		body          params.StatsUpdateRequest
		expectBody    map[string]interface{}
		previousMonth bool
	}{{
		path:   "stats/update",
		status: http.StatusOK,
		body: params.StatsUpdateRequest{
			Entries: []params.StatsUpdateEntry{{
				Timestamp:      time.Now(),
				CharmReference: charm.MustParseURL("~charmers/wordpress"),
			}}},
	}, {
		path:   "stats/update",
		status: http.StatusOK,
		body: params.StatsUpdateRequest{
			Entries: []params.StatsUpdateEntry{{
				Timestamp:      time.Now(),
				CharmReference: ref,
			}},
		},
	}, {
		path:   "stats/update",
		status: http.StatusOK,
		body: params.StatsUpdateRequest{
			Entries: []params.StatsUpdateEntry{{
				Timestamp:      time.Now().AddDate(0, -1, 0),
				CharmReference: ref,
			}},
		},
		previousMonth: true,
	}}

	s.addPublicCharm(c, storetesting.Charms.CharmDir("wordpress"), newResolvedURL("~charmers/precise/wordpress-23", 23))

	var countsBefore, countsAfter charmstore.AggregatedCounts
	for i, test := range tests {
		c.Logf("test %d. %s", i, test.path)

		var err error
		_, countsBefore, err = s.store.ArchiveDownloadCounts(ref)
		c.Assert(err, gc.Equals, nil)

		rec := httptesting.DoRequest(c, httptesting.DoRequestParams{
			Handler:  s.srv,
			URL:      storeURL(test.path),
			Method:   "PUT",
			Username: testUsername,
			Password: testPassword,
			JSONBody: test.body,
		})

		c.Assert(rec.Code, gc.Equals, test.status)

		_, countsAfter, err = s.store.ArchiveDownloadCounts(ref)
		c.Assert(err, gc.Equals, nil)
		c.Assert(countsAfter.Total-countsBefore.Total, gc.Equals, int64(1))
		if test.previousMonth {
			c.Assert(countsAfter.LastDay-countsBefore.LastDay, gc.Equals, int64(0))
		} else {
			c.Assert(countsAfter.LastDay-countsBefore.LastDay, gc.Equals, int64(1))
		}
	}
}

func (s *StatsSuite) TestServerStatsUpdateErrors(c *gc.C) {
	ref := charm.MustParseURL("~charmers/precise/wordpress-23")
	tests := []struct {
		path          string
		status        int
		body          params.StatsUpdateRequest
		expectMessage string
		expectCode    params.ErrorCode
		partialUpdate bool
	}{{
		path:   "stats/update",
		status: http.StatusInternalServerError,
		body: params.StatsUpdateRequest{
			Entries: []params.StatsUpdateEntry{{
				Timestamp:      time.Now(),
				CharmReference: charm.MustParseURL("~charmers/precise/unknown-23"),
			}},
		},
		expectMessage: `cannot find entity for url cs:~charmers/precise/unknown-23: no matching charm or bundle for cs:~charmers/precise/unknown-23`,
	}, {
		path:   "stats/update",
		status: http.StatusInternalServerError,
		body: params.StatsUpdateRequest{
			Entries: []params.StatsUpdateEntry{{
				Timestamp:      time.Now(),
				CharmReference: charm.MustParseURL("~charmers/precise/unknown-23"),
			}, {
				Timestamp:      time.Now(),
				CharmReference: charm.MustParseURL("~charmers/precise/wordpress-23"),
			}},
		},
		expectMessage: `cannot find entity for url cs:~charmers/precise/unknown-23: no matching charm or bundle for cs:~charmers/precise/unknown-23`,
		partialUpdate: true,
	}}

	s.addPublicCharm(c, storetesting.Charms.CharmDir("wordpress"), newResolvedURL("~charmers/precise/wordpress-23", 23))

	for i, test := range tests {
		c.Logf("test %d. %s", i, test.path)
		var countsBefore charmstore.AggregatedCounts
		if test.partialUpdate {
			var err error
			_, countsBefore, err = s.store.ArchiveDownloadCounts(ref)
			c.Assert(err, gc.Equals, nil)
		}
		httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
			Handler:      s.srv,
			URL:          storeURL(test.path),
			Method:       "PUT",
			Username:     testUsername,
			Password:     testPassword,
			JSONBody:     test.body,
			ExpectStatus: test.status,
			ExpectBody: params.Error{
				Message: test.expectMessage,
				Code:    test.expectCode,
			},
		})
		if test.partialUpdate {
			_, countsAfter, err := s.store.ArchiveDownloadCounts(ref)
			c.Assert(err, gc.Equals, nil)
			c.Assert(countsAfter.Total-countsBefore.Total, gc.Equals, int64(1))
			c.Assert(countsAfter.LastDay-countsBefore.LastDay, gc.Equals, int64(1))
		}
	}
}

func (s *StatsSuite) TestServerStatsUpdateNonAdmin(c *gc.C) {
	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler: s.srv,
		URL:     storeURL("stats/update"),
		Method:  "PUT",
		JSONBody: params.StatsUpdateRequest{
			Entries: []params.StatsUpdateEntry{{
				Timestamp:      time.Now(),
				CharmReference: charm.MustParseURL("~charmers/precise/wordpress-23"),
			}},
		},
		ExpectStatus: http.StatusUnauthorized,
		ExpectBody: &params.Error{
			Message: "authentication failed: missing HTTP auth header",
			Code:    params.ErrUnauthorized,
		},
	})
	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler:  s.srv,
		URL:      storeURL("stats/update"),
		Method:   "PUT",
		Username: "brad",
		Password: "pitt",
		JSONBody: params.StatsUpdateRequest{
			Entries: []params.StatsUpdateEntry{{
				Timestamp:      time.Now(),
				CharmReference: charm.MustParseURL("~charmers/precise/wordpress-23"),
			}},
		},
		ExpectStatus: http.StatusUnauthorized,
		ExpectBody: &params.Error{
			Message: "invalid user name or password",
			Code:    params.ErrUnauthorized,
		},
	})
}

func (s *StatsSuite) TestStatsEnabled(c *gc.C) {
	statsEnabled := func(url string) bool {
		req, _ := http.NewRequest("GET", url, nil)
		return v5.StatsEnabled(req)
	}
	c.Assert(statsEnabled("http://foo.com"), gc.Equals, true)
	c.Assert(statsEnabled("http://foo.com?stats=1"), gc.Equals, true)
	c.Assert(statsEnabled("http://foo.com?stats=0"), gc.Equals, false)
}
