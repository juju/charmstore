// Copyright 2014 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package charmstore // import "gopkg.in/juju/charmstore.v5/internal/charmstore"

import (
	"strconv"
	"sync"
	"time"

	"gopkg.in/errgo.v1"
	"gopkg.in/juju/charm.v6"
	"gopkg.in/juju/charmrepo.v3/csclient/params"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"gopkg.in/juju/charmstore.v5/internal/router"
)

type stats struct {
	// Cache for statistics key words (two generations).
	cacheMu       sync.RWMutex
	statsIdNew    map[string]int
	statsIdOld    map[string]int
	statsTokenNew map[int]string
	statsTokenOld map[int]string
}

// Note that changing the StatsGranularity constant
// will not change the stats time granularity - it
// is defined for external code clarity.

// StatsGranularity holds the time granularity of statistics
// gathering. IncCounter(Async) calls within this duration
// may be aggregated.
const StatsGranularity = time.Minute

// The stats mechanism uses the following MongoDB collections:
//
//     juju.stat.counters - Counters for statistics
//     juju.stat.tokens   - Tokens used in statistics counter keys

func (s StoreDatabase) StatCounters() *mgo.Collection {
	return s.C("juju.stat.counters")
}

func (s StoreDatabase) StatTokens() *mgo.Collection {
	return s.C("juju.stat.tokens")
}

// key returns the compound statistics identifier that represents key.
// If write is true, the identifier will be created if necessary.
// Identifiers have a form similar to "ab:c:def:", where each section is a
// base-32 number that represents the respective word in key. This form
// allows efficiently indexing and searching for prefixes, while detaching
// the key content and size from the actual words used in key.
func (s *stats) key(db StoreDatabase, key []string, write bool) (string, error) {
	if len(key) == 0 {
		return "", errgo.New("store: empty statistics key")
	}
	tokens := db.StatTokens()
	skey := make([]byte, 0, len(key)*4)
	// Retry limit is mainly to prevent infinite recursion in edge cases,
	// such as if the database is ever run in read-only mode.
	// The logic below should deteministically stop in normal scenarios.
	var err error
	for i, retry := 0, 30; i < len(key) && retry > 0; retry-- {
		err = nil
		id, found := s.tokenId(key[i])
		if !found {
			var t tokenId
			err = tokens.Find(bson.D{{"t", key[i]}}).One(&t)
			if err == mgo.ErrNotFound {
				if !write {
					return "", errgo.WithCausef(nil, params.ErrNotFound, "")
				}
				t.Id, err = tokens.Count()
				if err != nil {
					continue
				}
				t.Id++
				t.Token = key[i]
				err = tokens.Insert(&t)
			}
			if err != nil {
				continue
			}
			s.cacheTokenId(t.Token, t.Id)
			id = t.Id
		}
		skey = strconv.AppendInt(skey, int64(id), 32)
		skey = append(skey, ':')
		i++
	}
	if err != nil {
		return "", err
	}
	return string(skey), nil
}

const statsTokenCacheSize = 1024

type tokenId struct {
	Id    int    `bson:"_id"`
	Token string `bson:"t"`
}

// cacheTokenId adds the id for token into the cache.
// The cache has two generations so that the least frequently used
// tokens are evicted regularly.
func (s *stats) cacheTokenId(token string, id int) {
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()
	// Can't possibly be >, but reviews want it for defensiveness.
	if len(s.statsIdNew) >= statsTokenCacheSize {
		s.statsIdOld = s.statsIdNew
		s.statsIdNew = nil
		s.statsTokenOld = s.statsTokenNew
		s.statsTokenNew = nil
	}
	if s.statsIdNew == nil {
		s.statsIdNew = make(map[string]int, statsTokenCacheSize)
		s.statsTokenNew = make(map[int]string, statsTokenCacheSize)
	}
	s.statsIdNew[token] = id
	s.statsTokenNew[id] = token
}

// tokenId returns the id for token from the cache, if found.
func (s *stats) tokenId(token string) (id int, found bool) {
	s.cacheMu.RLock()
	id, found = s.statsIdNew[token]
	if found {
		s.cacheMu.RUnlock()
		return
	}
	id, found = s.statsIdOld[token]
	s.cacheMu.RUnlock()
	if found {
		s.cacheTokenId(token, id)
	}
	return
}

// idToken returns the token for id from the cache, if found.
func (s *stats) idToken(id int) (token string, found bool) {
	s.cacheMu.RLock()
	token, found = s.statsTokenNew[id]
	if found {
		s.cacheMu.RUnlock()
		return
	}
	token, found = s.statsTokenOld[id]
	s.cacheMu.RUnlock()
	if found {
		s.cacheTokenId(token, id)
	}
	return
}

var counterEpoch = time.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC).Unix()

func timeToStamp(t time.Time) int32 {
	return int32(t.Unix() - counterEpoch)
}

// IncCounterAsync increases by one the counter associated with the composed
// key. The action is done in the background using a separate goroutine.
func (s *Store) IncCounterAsync(key []string) {
	s.Go(func(s *Store) {
		if err := s.IncCounter(key); err != nil {
			logger.Errorf("cannot increase stats counter for key %v: %v", key, err)
		}
	})
}

// IncCounter increases by one the counter associated with the composed key.
func (s *Store) IncCounter(key []string) error {
	return s.IncCounterAtTime(key, time.Now())
}

// IncCounterAtTime increases by one the counter associated with the composed
// key, associating it with the given time.
func (s *Store) IncCounterAtTime(key []string, t time.Time) error {
	skey, err := s.stats.key(s.DB, key, true)
	if err != nil {
		return err
	}

	// Round to the start of the minute so we get one document per minute at most.
	t = t.UTC().Add(-time.Duration(t.Second()) * time.Second)
	counters := s.DB.StatCounters()
	_, err = counters.Upsert(bson.D{{"k", skey}, {"t", timeToStamp(t)}}, bson.D{{"$inc", bson.D{{"c", 1}}}})
	return err
}

// CounterRequest represents a request to aggregate counter values.
type CounterRequest struct {
	// Key and Prefix determine the counter keys to match.
	// If Prefix is false, Key must match exactly. Otherwise, counters
	// must begin with Key and have at least one more key token.
	Key    []string
	Prefix bool

	// If List is true, matching counters are aggregated under their
	// prefixes instead of being returned as a single overall sum.
	//
	// For example, given the following counts:
	//
	//   {"a", "b"}: 1,
	//   {"a", "c"}: 3
	//   {"a", "c", "d"}: 5
	//   {"a", "c", "e"}: 7
	//
	// and assuming that Prefix is true, the following keys will
	// present the respective results if List is true:
	//
	//        {"a"} => {{"a", "b"}, 1, false},
	//                 {{"a", "c"}, 3, false},
	//                 {{"a", "c"}, 12, true}
	//   {"a", "c"} => {{"a", "c", "d"}, 3, false},
	//                 {{"a", "c", "e"}, 5, false}
	//
	// If List is false, the same key prefixes will present:
	//
	//        {"a"} => {{"a"}, 16, true}
	//   {"a", "c"} => {{"a", "c"}, 12, false}
	//
	List bool

	// By defines the period covered by each aggregated data point.
	// If unspecified, it defaults to ByAll, which aggregates all
	// matching data points in a single entry.
	By CounterRequestBy

	// Start, if provided, changes the query so that only data points
	// ocurring at the given time or afterwards are considered.
	Start time.Time

	// Stop, if provided, changes the query so that only data points
	// ocurring at the given time or before are considered.
	Stop time.Time
}

type CounterRequestBy int

const (
	ByAll CounterRequestBy = iota
	ByDay
	ByWeek
)

type Counter struct {
	Key    []string
	Prefix bool
	Count  int64
	Time   time.Time
}

// Counters aggregates and returns counter values according to the provided request.
func (s *Store) Counters(req *CounterRequest) ([]Counter, error) {
	// Return 0 value or not-present values for all statistics.
	if req.List {
		return nil, nil
	}
	return []Counter{{
		Key:    req.Key,
		Prefix: req.Prefix,
		Count:  0,
	}}, nil
}

// EntityStatsKey returns a stats key for the given charm or bundle
// reference and the given kind.
// Entity stats keys are generated using the following schema:
//   kind:series:name:user:revision
// where user can be empty (for promulgated charms/bundles) and revision is
// optional (e.g. when uploading an entity the revision is not specified).
// For instance, entities' stats can then be retrieved like the following:
//   - kind:utopic:* -> all charms of a specific series;
//   - kind:trusty:django:* -> all revisions and user variations of a charm;
//   - kind:trusty:django::* -> all revisions of a promulgated charm;
//   - kind:trusty:django::42 -> a specific promulgated charm;
//   - kind:trusty:django:who:* -> all revisions of a user owned charm;
//   - kind:trusty:django:who:42 -> a specific user owned charm;
// The above also applies to bundles (where the series is "bundle").
func EntityStatsKey(url *charm.URL, kind string) []string {
	key := []string{kind, url.Series, url.Name, url.User}
	if url.Revision != -1 {
		key = append(key, strconv.Itoa(url.Revision))
	}
	return key
}

// AggregatedCounts contains counts for a statistic aggregated over the
// lastDay, lastWeek, lastMonth and all time.
type AggregatedCounts struct {
	LastDay, LastWeek, LastMonth, Total int64
}

// ArchiveDownloadCounts calculates the aggregated download counts for
// a charm or bundle.
func (s *Store) ArchiveDownloadCounts(id *charm.URL, refresh bool) (thisRevision, allRevisions AggregatedCounts, err error) {
	// Return 0 value counts for all archive downloads.
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
	key := EntityStatsKey(&id.URL, params.StatsArchiveDownload)
	if err := s.IncCounterAtTime(key, t); err != nil {
		return errgo.Notef(err, "cannot increase stats counter for %v", key)
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
		key := EntityStatsKey(id.PromulgatedURL(), params.StatsArchiveDownloadPromulgated)
		if err := s.IncCounterAtTime(key, t); err != nil {
			return errgo.Notef(err, "cannot increase stats counter for %v", key)
		}
	}
	// TODO(mhilton) when this charmstore is being used by juju, find a more
	// efficient way to update the download statistics for search.
	if err := s.UpdateSearch(id); err != nil {
		return errgo.Notef(err, "cannot update search record for %v", id)
	}
	return nil
}
