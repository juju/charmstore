// Copyright 2014 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package charmstore_test

import (
	"time"

	"github.com/juju/charm/v7"
	jujutesting "github.com/juju/testing"
	jc "github.com/juju/testing/checkers"
	gc "gopkg.in/check.v1"

	"gopkg.in/juju/charmstore.v5/internal/charmstore"
	"gopkg.in/juju/charmstore.v5/internal/router"
	"gopkg.in/juju/charmstore.v5/internal/storetesting"
)

type StatsSuite struct {
	jujutesting.IsolatedMgoSuite
	store *charmstore.Store
}

var _ = gc.Suite(&StatsSuite{})

func (s *StatsSuite) SetUpTest(c *gc.C) {
	s.IsolatedMgoSuite.SetUpTest(c)
	pool, err := charmstore.NewPool(s.Session.DB("foo"), nil, nil, charmstore.ServerParams{})
	c.Assert(err, gc.Equals, nil)
	s.store = pool.Store()
	pool.Close()
}

func (s *StatsSuite) TearDownTest(c *gc.C) {
	if s.store != nil {
		s.store.Close()
	}
	s.IsolatedMgoSuite.TearDownTest(c)
}

type testStatsEntity struct {
	id        *router.ResolvedURL
	lastDay   int
	lastWeek  int
	lastMonth int
	total     int
}

var archiveDownloadCountsTests = []struct {
	about              string
	charms             []testStatsEntity
	id                 *charm.URL
	expectThisRevision charmstore.AggregatedCounts
	expectAllRevisions charmstore.AggregatedCounts
}{{
	about: "single revision",
	charms: []testStatsEntity{{
		id:        charmstore.MustParseResolvedURL("~charmers/trusty/wordpress-0"),
		lastDay:   1,
		lastWeek:  2,
		lastMonth: 3,
		total:     4,
	}},
	id: charm.MustParseURL("~charmers/trusty/wordpress-0"),
	expectThisRevision: charmstore.AggregatedCounts{
		LastDay:   1,
		LastWeek:  3,
		LastMonth: 6,
		Total:     10,
	},
	expectAllRevisions: charmstore.AggregatedCounts{
		LastDay:   1,
		LastWeek:  3,
		LastMonth: 6,
		Total:     10,
	},
}, {
	about: "multiple revisions",
	charms: []testStatsEntity{{
		id:        charmstore.MustParseResolvedURL("~charmers/trusty/wordpress-0"),
		lastDay:   1,
		lastWeek:  2,
		lastMonth: 3,
		total:     4,
	}, {
		id:        charmstore.MustParseResolvedURL("~charmers/trusty/wordpress-1"),
		lastDay:   2,
		lastWeek:  3,
		lastMonth: 4,
		total:     5,
	}},
	id: charm.MustParseURL("~charmers/trusty/wordpress-1"),
	expectThisRevision: charmstore.AggregatedCounts{
		LastDay:   2,
		LastWeek:  5,
		LastMonth: 9,
		Total:     14,
	},
	expectAllRevisions: charmstore.AggregatedCounts{
		LastDay:   3,
		LastWeek:  8,
		LastMonth: 15,
		Total:     24,
	},
}, {
	about: "promulgated revision",
	charms: []testStatsEntity{{
		id:        charmstore.MustParseResolvedURL("0 ~charmers/trusty/wordpress-0"),
		lastDay:   1,
		lastWeek:  2,
		lastMonth: 3,
		total:     4,
	}},
	id: charm.MustParseURL("trusty/wordpress-0"),
	expectThisRevision: charmstore.AggregatedCounts{
		LastDay:   1,
		LastWeek:  3,
		LastMonth: 6,
		Total:     10,
	},
	expectAllRevisions: charmstore.AggregatedCounts{
		LastDay:   1,
		LastWeek:  3,
		LastMonth: 6,
		Total:     10,
	},
}, {
	about: "promulgated revision with changed owner",
	charms: []testStatsEntity{{
		id:        charmstore.MustParseResolvedURL("0 ~charmers/trusty/wordpress-0"),
		lastDay:   1,
		lastWeek:  10,
		lastMonth: 100,
		total:     1000,
	}, {
		id:        charmstore.MustParseResolvedURL("~charmers/trusty/wordpress-1"),
		lastDay:   2,
		lastWeek:  20,
		lastMonth: 200,
		total:     2000,
	}, {
		id:        charmstore.MustParseResolvedURL("~wordpress-charmers/trusty/wordpress-0"),
		lastDay:   3,
		lastWeek:  30,
		lastMonth: 300,
		total:     3000,
	}, {
		id:        charmstore.MustParseResolvedURL("1 ~wordpress-charmers/trusty/wordpress-1"),
		lastDay:   4,
		lastWeek:  40,
		lastMonth: 400,
		total:     4000,
	}},
	id: charm.MustParseURL("trusty/wordpress-1"),
	expectThisRevision: charmstore.AggregatedCounts{
		LastDay:   4,
		LastWeek:  44,
		LastMonth: 444,
		Total:     4444,
	},
	expectAllRevisions: charmstore.AggregatedCounts{
		LastDay:   5,
		LastWeek:  55,
		LastMonth: 555,
		Total:     5555,
	},
}}

func (s *StatsSuite) TestArchiveDownloadCounts(c *gc.C) {
	for i, test := range archiveDownloadCountsTests {
		c.Logf("%d: %s", i, test.about)
		// Clear everything
		s.store.DB.Entities().RemoveAll(nil)
		s.store.DB.DownloadCounts().RemoveAll(nil)
		for _, charm := range test.charms {
			ch := storetesting.Charms.CharmDir(charm.id.URL.Name)
			err := s.store.AddCharmWithArchive(charm.id, ch)
			c.Assert(err, gc.Equals, nil)
			now := time.Now()
			setDownloadCounts(c, s.store, charm.id, now, charm.lastDay)
			setDownloadCounts(c, s.store, charm.id, lastWeekTime(now), charm.lastWeek)
			setDownloadCounts(c, s.store, charm.id, lastMonthTime(now), charm.lastMonth)
			setDownloadCounts(c, s.store, charm.id, now.Add(-100*24*time.Hour), charm.total)
		}
		thisRevision, allRevisions, err := s.store.ArchiveDownloadCounts(test.id)
		c.Assert(err, gc.Equals, nil)
		c.Assert(thisRevision, jc.DeepEquals, test.expectThisRevision)
		c.Assert(allRevisions, jc.DeepEquals, test.expectAllRevisions)
	}
}

func setDownloadCounts(c *gc.C, s *charmstore.Store, id *router.ResolvedURL, t time.Time, n int) {
	for i := 0; i < n; i++ {
		err := s.IncrementDownloadCountsAtTime(id, t)
		c.Assert(err, gc.Equals, nil)
	}
}

func (s *StatsSuite) TestIncrementDownloadCounts(c *gc.C) {
	ch := storetesting.Charms.CharmDir("wordpress")
	id := charmstore.MustParseResolvedURL("0 ~charmers/trusty/wordpress-1")
	err := s.store.AddCharmWithArchive(id, ch)
	c.Assert(err, gc.Equals, nil)
	err = s.store.IncrementDownloadCounts(id)
	c.Assert(err, gc.Equals, nil)
	expect := charmstore.AggregatedCounts{
		LastDay:   1,
		LastWeek:  1,
		LastMonth: 1,
		Total:     1,
	}
	thisRevision, allRevisions, err := s.store.ArchiveDownloadCounts(charm.MustParseURL("~charmers/trusty/wordpress-1"))
	c.Assert(err, gc.Equals, nil)
	c.Assert(thisRevision, jc.DeepEquals, expect)
	c.Assert(allRevisions, jc.DeepEquals, expect)
	thisRevision, allRevisions, err = s.store.ArchiveDownloadCounts(charm.MustParseURL("trusty/wordpress-0"))
	c.Assert(err, gc.Equals, nil)
	c.Assert(thisRevision, jc.DeepEquals, expect)
	c.Assert(allRevisions, jc.DeepEquals, expect)
}

func (s *StatsSuite) TestIncrementDownloadCountsOnPromulgatedMultiSeriesCharm(c *gc.C) {
	ch := storetesting.Charms.CharmDir("multi-series")
	id := charmstore.MustParseResolvedURL("0 ~charmers/wordpress-1")
	err := s.store.AddCharmWithArchive(id, ch)
	c.Assert(err, gc.Equals, nil)
	err = s.store.IncrementDownloadCounts(id)
	c.Assert(err, gc.Equals, nil)
	expect := charmstore.AggregatedCounts{
		LastDay:   1,
		LastWeek:  1,
		LastMonth: 1,
		Total:     1,
	}
	thisRevision, allRevisions, err := s.store.ArchiveDownloadCounts(charm.MustParseURL("~charmers/wordpress-1"))
	c.Assert(err, gc.Equals, nil)
	c.Assert(thisRevision, jc.DeepEquals, expect)
	c.Assert(allRevisions, jc.DeepEquals, expect)
	thisRevision, allRevisions, err = s.store.ArchiveDownloadCounts(charm.MustParseURL("wordpress-0"))
	c.Assert(err, gc.Equals, nil)
	c.Assert(thisRevision, jc.DeepEquals, expect)
	c.Assert(allRevisions, jc.DeepEquals, expect)
}

func (s *StatsSuite) TestIncrementDownloadCountsOnIdWithPreferredSeries(c *gc.C) {
	ch := storetesting.Charms.CharmDir("multi-series")
	id := charmstore.MustParseResolvedURL("0 ~charmers/wordpress-1")
	id.PreferredSeries = "trusty"
	err := s.store.AddCharmWithArchive(id, ch)
	c.Assert(err, gc.Equals, nil)
	err = s.store.IncrementDownloadCounts(id)
	c.Assert(err, gc.Equals, nil)
	expect := charmstore.AggregatedCounts{
		LastDay:   1,
		LastWeek:  1,
		LastMonth: 1,
		Total:     1,
	}
	thisRevision, allRevisions, err := s.store.ArchiveDownloadCounts(charm.MustParseURL("~charmers/wordpress-1"))
	c.Assert(err, gc.Equals, nil)
	c.Assert(thisRevision, jc.DeepEquals, expect)
	c.Assert(allRevisions, jc.DeepEquals, expect)
	thisRevision, allRevisions, err = s.store.ArchiveDownloadCounts(charm.MustParseURL("wordpress-0"))
	c.Assert(err, gc.Equals, nil)
	c.Assert(thisRevision, jc.DeepEquals, expect)
	c.Assert(allRevisions, jc.DeepEquals, expect)
}

// lastWeekTime calculates a time that is in the current week, but not in
// the current day.
func lastWeekTime(t time.Time) time.Time {
	if t.Weekday() == time.Monday {
		return t.AddDate(0, 0, 1)
	}
	return t.AddDate(0, 0, -1)
}

// lastMonthTime calculates a time that is in the current month, but not in
// the current week.
func lastMonthTime(t time.Time) time.Time {
	if t.Day() > 14 {
		return t.AddDate(0, 0, -14)
	}
	return t.AddDate(0, 0, 14)
}
