// Copyright 2013, 2014 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package charmstore

var (
	TimeToStamp = timeToStamp
)

// StatsCacheEvictAll removes everything from the stats cache.
func StatsCacheEvictAll(s *Store) {
	s.pool.statsCache.EvictAll()
}
