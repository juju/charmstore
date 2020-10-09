// Copyright 2014 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package charmstore // import "gopkg.in/juju/charmstore.v5/internal/charmstore"

import (
	"fmt"
	"time"

	"gopkg.in/errgo.v1"
	"gopkg.in/juju/charmstore.v5/internal/charm"
	"gopkg.in/mgo.v2/bson"

	"gopkg.in/juju/charmstore.v5/internal/mongodoc"
	"gopkg.in/juju/charmstore.v5/internal/router"
)

// AggregatedCounts contains counts for a statistic aggregated over the
// lastDay, lastWeek, lastMonth and all time.
type AggregatedCounts struct {
	LastDay, LastWeek, LastMonth, Total int64
}

// ArchiveDownloadCounts calculates the aggregated download counts for
// a charm or bundle.
func (s *Store) ArchiveDownloadCounts(id *charm.URL) (thisRevision, allRevisions AggregatedCounts, err error) {
	return s.ArchiveDownloadCountsAtTime(id, time.Now())
}

func (s *Store) ArchiveDownloadCountsAtTime(id *charm.URL, t time.Time) (thisRevision, allRevisions AggregatedCounts, err error) {
	withRevision := id.String()
	id1 := *id
	id1.Revision = -1
	withoutRevision := id1.String()

	day, _ := currentDay(t)
	week, _ := currentWeek(t)
	month, _ := currentMonth(t)

	it := s.DB.DownloadCounts().Find(bson.D{{"$or", []bson.D{{{"id", withRevision}}, {{"id", withoutRevision}}}}}).Iter()
	defer it.Close()

	var dc mongodoc.DownloadCount
	for it.Next(&dc) {
		switch {
		case dc.ID == withRevision && dc.Period == "":
			thisRevision.Total = dc.Count
		case dc.ID == withRevision && dc.Period == day:
			thisRevision.LastDay = dc.Count
		case dc.ID == withRevision && dc.Period == week:
			thisRevision.LastWeek = dc.Count
		case dc.ID == withRevision && dc.Period == month:
			thisRevision.LastMonth = dc.Count
		case dc.ID == withoutRevision && dc.Period == "":
			allRevisions.Total = dc.Count
		case dc.ID == withoutRevision && dc.Period == day:
			allRevisions.LastDay = dc.Count
		case dc.ID == withoutRevision && dc.Period == week:
			allRevisions.LastWeek = dc.Count
		case dc.ID == withoutRevision && dc.Period == month:
			allRevisions.LastMonth = dc.Count
		}
	}

	err = errgo.Mask(it.Err())
	return
}

// IncrementDownloadCountsAsync updates the download statistics for entity id in both
// the statistics database and the search database. The action is done in the
// background using a separate goroutine.
func (s *Store) IncrementDownloadCountsAsync(id *router.ResolvedURL) {
	s.Go(func(s *Store) {
		if err := s.IncrementDownloadCounts(id); err != nil {
			logger.Errorf("cannot increase download counter for %v: %s", id, err)
		}
	})
}

// IncrementDownloadCounts updates the download statistics for entity id in both
// the statistics database and the search database.
func (s *Store) IncrementDownloadCounts(id *router.ResolvedURL) error {
	return s.IncrementDownloadCountsAtTime(id, time.Now())
}

// IncrementDownloadCountsAtTime updates the download statistics for entity id in both
// the statistics database and the search database, associating it with the given time.
func (s *Store) IncrementDownloadCountsAtTime(id *router.ResolvedURL, t time.Time) error {
	if err := s.incrementDownloadCountsAtTime(&id.URL, t); err != nil {
		return errgo.Mask(err)
	}
	if id.PromulgatedRevision == -1 {
		// Check that the id really is for an unpromulgated entity.
		// This unfortunately adds an extra round trip to the database,
		// but as incrementing statistics is performed asynchronously
		// it will not be in the critical path.
		entity, err := s.FindEntity(id, FieldSelector("promulgated-revision"))
		if err != nil {
			return errgo.Notef(err, "cannot find entity %v", &id.URL)
		}
		id.PromulgatedRevision = entity.PromulgatedRevision
	}

	if id.PromulgatedRevision != -1 {
		if err := s.incrementDownloadCountsAtTime(id.PromulgatedURL(), t); err != nil {
			return errgo.Mask(err)
		}
	}

	// TODO(mhilton) when this charmstore is being used by juju, find a more
	// efficient way to update the download statistics for search.
	if err := s.UpdateSearch(id); err != nil {
		return errgo.Notef(err, "cannot update search record for %v", id)
	}
	return nil
}

func (s *Store) incrementDownloadCountsAtTime(url *charm.URL, t time.Time) error {
	day, dayExpires := currentDay(t)
	week, weekExpires := currentWeek(t)
	month, monthExpires := currentMonth(t)

	withRevision := url.String()
	withoutRevisionURL := *url
	withoutRevisionURL.Revision = -1
	withoutRevision := withoutRevisionURL.String()

	dcs := []mongodoc.DownloadCount{{
		ID:    withRevision,
		Count: 1,
	}, {
		ID:      withRevision,
		Period:  day,
		Count:   1,
		Expires: &dayExpires,
	}, {
		ID:      withRevision,
		Period:  week,
		Count:   1,
		Expires: &weekExpires,
	}, {
		ID:      withRevision,
		Period:  month,
		Count:   1,
		Expires: &monthExpires,
	}, {
		ID:    withoutRevision,
		Count: 1,
	}, {
		ID:      withoutRevision,
		Period:  day,
		Count:   1,
		Expires: &dayExpires,
	}, {
		ID:      withoutRevision,
		Period:  week,
		Count:   1,
		Expires: &weekExpires,
	}, {
		ID:      withoutRevision,
		Period:  month,
		Count:   1,
		Expires: &monthExpires,
	}}

	for _, dc := range dcs {
		if err := s.incrementDownloadCount(dc); err != nil {
			return errgo.Mask(err)
		}
	}
	return nil
}

// currentDay returns the day that the given time occurs in along with
// the time at which that count should expire.
func currentDay(t time.Time) (period string, expires time.Time) {
	y, m, d := t.UTC().Date()
	return fmt.Sprintf("%04d-%02d-%02d", y, m, d), time.Date(y, m, d, 0, 0, 0, 0, time.UTC).AddDate(0, 0, 1)
}

// currentWeek returns the ISO 8601 week that the given time occurs in
// along with the time at which that count should expire. f
func currentWeek(t time.Time) (period string, expires time.Time) {
	expires = t.UTC().Round(24 * time.Hour)
	switch expires.Weekday() {
	case time.Sunday:
		expires = expires.AddDate(0, 0, 1)
	case time.Monday:
		expires = expires.AddDate(0, 0, 7)
	case time.Tuesday:
		expires = expires.AddDate(0, 0, 6)
	case time.Wednesday:
		expires = expires.AddDate(0, 0, 5)
	case time.Thursday:
		expires = expires.AddDate(0, 0, 4)
	case time.Friday:
		expires = expires.AddDate(0, 0, 3)
	case time.Saturday:
		expires = expires.AddDate(0, 0, 2)
	}

	y, w := t.UTC().ISOWeek()
	return fmt.Sprintf("%04d-W%02d", y, w), expires
}

// currentMonth returns the month that the given time occurs in along with
// the time at which that count should expire.
func currentMonth(t time.Time) (period string, expires time.Time) {
	y, m, _ := t.UTC().Date()
	return fmt.Sprintf("%04d-%02d", y, m), time.Date(y, m, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 1, 0)
}

func (s *Store) incrementDownloadCount(dc mongodoc.DownloadCount) error {
	query := make(bson.D, 1, 2)
	update := make(bson.D, 1, 2)

	query[0] = bson.DocElem{"id", dc.ID}
	if dc.Period != "" {
		query = append(query, bson.DocElem{"period", dc.Period})
	}
	update[0] = bson.DocElem{"$inc", bson.D{{"count", dc.Count}}}
	if dc.Expires != nil {
		update = append(update, bson.DocElem{"$setOnInsert", bson.D{{"expires", dc.Expires}}})
	}

	_, err := s.DB.DownloadCounts().Upsert(query, update)
	return errgo.Mask(err)
}
