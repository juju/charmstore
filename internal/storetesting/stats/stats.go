// Copyright 2014 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package stats // import "gopkg.in/juju/charmstore.v5/internal/storetesting/stats"

import (
	"time"

	gc "gopkg.in/check.v1"

	"gopkg.in/juju/charmstore.v5/internal/charm"
	"gopkg.in/juju/charmstore.v5/internal/charmstore"
)

// CheckSearchTotalDownloads checks that the search index is properly updated.
// It retries a few times as they are generally updated in background.
func CheckSearchTotalDownloads(c *gc.C, store *charmstore.Store, id *charm.URL, expected int64) {
	var doc *charmstore.SearchDoc
	for retry := 0; retry < 10; retry++ {
		var err error
		time.Sleep(100 * time.Millisecond)
		doc, err = store.ES.GetSearchDocument(id)
		c.Assert(err, gc.Equals, nil)
		if doc.TotalDownloads == expected {
			if expected == 0 && retry < 2 {
				continue // Wait a bit to make sure.
			}
			return
		}
	}
	c.Errorf("total downloads for %#v is %d, want %d", id, doc.TotalDownloads, expected)
}

// CheckTotalDownloads checks that the download counts are updated .
func CheckTotalDownloads(c *gc.C, store *charmstore.Store, id *charm.URL, expected int64) {
	var counts charmstore.AggregatedCounts
	for retry := 0; retry < 10; retry++ {
		var err error
		time.Sleep(100 * time.Millisecond)
		counts, _, err = store.ArchiveDownloadCounts(id)
		c.Assert(err, gc.Equals, nil)
		if counts.Total == expected {
			if expected == 0 && retry < 2 {
				continue // Wait a bit to make sure.
			}
			return
		}
	}
	c.Errorf("total downloads for %#v is %d, want %d", id, counts.Total, expected)
}

// ThisWeek processes the given day-count mappings, and calculates how
// many of the counts occurred in the current week. This is necessary as
// weekly stats are grouped by ISO8601 week, and therefore the value
// changes depending on how far through the week you are.
//
// Each input map is keyed by how many days in the past the count
// occurred, the value being the count that occured on that day. It is
// fine for more than one map to contain the same key.
func ThisWeek(args ...map[int]int) int64 {
	days := 0
	switch time.Now().Weekday() {
	case time.Tuesday:
		days = 1
	case time.Wednesday:
		days = 2
	case time.Thursday:
		days = 3
	case time.Friday:
		days = 4
	case time.Saturday:
		days = 5
	case time.Sunday:
		days = 6
	}

	total := int64(0)
	for _, arg := range args {
		for i, n := range arg {
			if i <= days {
				total += int64(n)
			}
		}
	}

	return total
}

// ThisMonth processes the given day-count mappings, and calculates how
// many of the counts occurred in the current month. This is necessary as
// stats are grouped by the current month, and therefore the value
// changes depending on how far through the month you are.
//
// Each input map is keyed by how many days in the past the count
// occurred, the value being the count that occured on that day. It is
// fine for more than one map to contain the same key.
func ThisMonth(args ...map[int]int) int64 {
	days := time.Now().Day()

	total := int64(0)
	for _, arg := range args {
		for i, n := range arg {
			if i < days {
				total += int64(n)
			}
		}
	}

	return total
}
