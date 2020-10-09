// Copyright 2014 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package charmstore_test

import (
	"time"

	jujutesting "github.com/juju/testing"
	jc "github.com/juju/testing/checkers"
	gc "gopkg.in/check.v1"

	"gopkg.in/juju/charmstore.v5/internal/charm"
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
		LastWeek:  weekCount(1, 2),
		LastMonth: monthCount(1, 2, 3),
		Total:     10,
	},
	expectAllRevisions: charmstore.AggregatedCounts{
		LastDay:   1,
		LastWeek:  weekCount(1, 2),
		LastMonth: monthCount(1, 2, 3),
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
		LastWeek:  weekCount(2, 3),
		LastMonth: monthCount(2, 3, 4),
		Total:     14,
	},
	expectAllRevisions: charmstore.AggregatedCounts{
		LastDay:   3,
		LastWeek:  weekCount(1+2, 2+3),
		LastMonth: monthCount(1+2, 2+3, 3+4),
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
		LastWeek:  weekCount(1, 2),
		LastMonth: monthCount(1, 2, 3),
		Total:     10,
	},
	expectAllRevisions: charmstore.AggregatedCounts{
		LastDay:   1,
		LastWeek:  weekCount(1, 2),
		LastMonth: monthCount(1, 2, 3),
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
		LastWeek:  weekCount(4, 40),
		LastMonth: monthCount(4, 40, 400),
		Total:     4444,
	},
	expectAllRevisions: charmstore.AggregatedCounts{
		LastDay:   5,
		LastWeek:  weekCount(1+4, 10+40),
		LastMonth: monthCount(1+4, 10+40, 100+400),
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
			setDownloadCounts(c, s.store, charm.id, now.AddDate(0, 0, -1), charm.lastWeek)
			setDownloadCounts(c, s.store, charm.id, now.AddDate(0, 0, -7), charm.lastMonth)
			setDownloadCounts(c, s.store, charm.id, now.AddDate(0, 0, -100), charm.total)
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

// weekCount calculates how many of the added statistics count as being
// in the current week. A week starts on a Monday so only a day count
// will fit into the current week, otherwise the day and week count are
// used.
func weekCount(day, week int) int64 {
	if time.Now().Weekday() == time.Monday {
		return int64(day)
	}
	return int64(day + week)
}

// monthCount calculates how many of the added statistics count as being
// in the current week. The week count is added to the database 1 day ago
// and the month count 7 days ago. These counts aren't included if they
// occured in the previous month.
func monthCount(day, week, month int) int64 {
	dom := time.Now().Day()
	switch {
	case dom == 1:
		return int64(day)
	case dom < 7:
		return int64(day + week)
	default:
		return int64(day + week + month)
	}
}
