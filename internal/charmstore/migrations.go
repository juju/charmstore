// Copyright 2014 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package charmstore // import "gopkg.in/juju/charmstore.v5-unstable/internal/charmstore"

import (
	"time"

	"github.com/juju/utils/parallel"
	"gopkg.in/errgo.v1"
	"gopkg.in/juju/charm.v6-unstable"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"gopkg.in/juju/charmstore.v5-unstable/internal/mongodoc"
)

const (
	migrationAddSupportedSeries      mongodoc.MigrationName = "add supported series"
	migrationAddDevelopment          mongodoc.MigrationName = "add development"
	migrationAddDevelopmentACLs      mongodoc.MigrationName = "add development acls"
	migrationFixBogusPromulgatedURL  mongodoc.MigrationName = "fix promulgate url"
	migrationAddPreV5CompatBlobBogus mongodoc.MigrationName = "add pre-v5 compatibility blobs"
	migrationAddPreV5CompatBlob      mongodoc.MigrationName = "add pre-v5 compatibility blobs; second try"
	migrationNewChannelsModel        mongodoc.MigrationName = "new channels model"
	migrationStats                   mongodoc.MigrationName = "remove legacy download stats"
	migrationEdgeEntities            mongodoc.MigrationName = "rename development to edge in entities"
	migrationEdgeBaseEntities        mongodoc.MigrationName = "rename development to edge in base entities"
	migrationPublishedEntities       mongodoc.MigrationName = "include published status in a single entity field"
	migrationCandidateBetaChannels   mongodoc.MigrationName = "populate candidate and beta channel ACLs"
	migrationRevisionsCollection     mongodoc.MigrationName = "populate revisions collection"
)

// migrations holds all the migration functions that are executed in the order
// they are defined when the charm store server is started. Each migration is
// associated with a name that is used to check whether the migration has been
// already run. To introduce a new database migration, add the corresponding
// migration name and function to this list, and update the
// TestMigrateMigrationList test in migration_test.go adding the new name(s).
// Note that migration names must be unique across the list.
//
// A migration entry may have a nil migration function if the migration
// is obsolete. Obsolete migrations should never be removed entirely,
// otherwise the charmstore will see the old migrations in the table
// and refuse to start up because it thinks that it's running an old
// version of the charm store on a newer version of the database.
var migrations = []migration{{
	name: "entity ids denormalization",
}, {
	name: "base entities creation",
}, {
	name: "read acl creation",
}, {
	name: "write acl creation",
}, {
	name: migrationAddSupportedSeries,
}, {
	name: migrationAddDevelopment,
}, {
	name: migrationAddDevelopmentACLs,
}, {
	name: migrationFixBogusPromulgatedURL,
}, {
	// The original migration that attempted to do this actually did
	// nothing, so leave it here but use a new name for the
	// fixed version.
	name: migrationAddPreV5CompatBlobBogus,
}, {
	name: migrationAddPreV5CompatBlob,
}, {
	name: migrationNewChannelsModel,
}, {
	name: migrationStats,
}, {
	name: migrationEdgeEntities,
}, {
	name: migrationEdgeBaseEntities,
}, {
	name: migrationPublishedEntities,
}, {
	name: migrationCandidateBetaChannels,
}, {
	name:    migrationRevisionsCollection,
	migrate: migrateRevisionsCollection,
}}

// migration holds a migration function with its corresponding name.
type migration struct {
	name    mongodoc.MigrationName
	migrate func(StoreDatabase) error
}

// Migrate starts the migration process using the given database.
func migrate(db StoreDatabase) error {
	db = db.copy()
	defer db.Close()
	db.Session.SetSocketTimeout(10 * time.Minute)
	// Set the socket timeout back to the default value of one minute.
	defer db.Session.SetSocketTimeout(1 * time.Minute)
	// Retrieve already executed migrations.
	executed, err := getExecuted(db)
	if err != nil {
		return errgo.Mask(err)
	}

	// Explicitly create the collection in case there are no migrations
	// so that the tests that expect the migrations collection to exist
	// will pass. We ignore the error because we'll get one if the
	// collection already exists and there's no special type or value
	// for that (and if it's a genuine error, we'll catch the problem later
	// anyway).
	db.Migrations().Create(&mgo.CollectionInfo{})
	// Execute required migrations.
	for _, m := range migrations {
		if executed[m.name] || m.migrate == nil {
			logger.Debugf("skipping already executed migration: %s", m.name)
			continue
		}
		logger.Infof("starting migration: %s", m.name)
		if err := m.migrate(db); err != nil {
			return errgo.Notef(err, "error executing migration: %s", m.name)
		}
		if err := setExecuted(db, m.name); err != nil {
			return errgo.Mask(err)
		}
		logger.Infof("migration completed: %s", m.name)
	}
	return nil
}

func getExecuted(db StoreDatabase) (map[mongodoc.MigrationName]bool, error) {
	// Retrieve the already executed migration names.
	executed := make(map[mongodoc.MigrationName]bool)
	var doc mongodoc.Migration
	if err := db.Migrations().Find(nil).Select(bson.D{{"executed", 1}}).One(&doc); err != nil {
		if err == mgo.ErrNotFound {
			return executed, nil
		}
		return nil, errgo.Notef(err, "cannot retrieve executed migrations")
	}

	names := make(map[mongodoc.MigrationName]bool, len(migrations))
	for _, m := range migrations {
		names[m.name] = true
	}
	for _, name := range doc.Executed {
		name := mongodoc.MigrationName(name)
		// Check that the already executed migrations are known.
		if !names[name] {
			return nil, errgo.Newf("found unknown migration %q; running old charm store code on newer charm store database?", name)
		}
		// Collect the name of the executed migration.
		executed[name] = true
	}
	return executed, nil
}

func setExecuted(db StoreDatabase, name mongodoc.MigrationName) error {
	if _, err := db.Migrations().Upsert(nil, bson.D{{
		"$addToSet", bson.D{{"executed", name}},
	}}); err != nil {
		return errgo.Notef(err, "cannot add %s to executed migrations", name)
	}
	return nil
}

// migrateRevisionsCollection populates the revisions collection
// from the entities in the database.
func migrateRevisionsCollection(db StoreDatabase) error {
	revs := make(map[string]int)
	set := func(url *charm.URL) {
		rev := url.Revision
		url = url.WithRevision(-1)
		urlStr := url.String()
		if oldRev, ok := revs[urlStr]; !ok || rev > oldRev {
			revs[urlStr] = rev
		}
	}
	iter := db.Entities().Find(nil).Select(bson.M{"baseurl": 1, "promulgated-url": 1}).Iter()
	var entity mongodoc.Entity
	for iter.Next(&entity) {
		set(entity.URL)
		if entity.PromulgatedURL != nil {
			set(entity.PromulgatedURL)
		}
	}
	if err := iter.Err(); err != nil {
		return errgo.Notef(err, "could not iterate through all entities")
	}
	col := db.Revisions()
	run := parallel.NewRun(20)
	for urlStr, rev := range revs {
		urlStr, rev := urlStr, rev
		run.Do(func() error {
			url := charm.MustParseURL(urlStr)
			err := col.Insert(mongodoc.LatestRevision{
				URL:      url,
				BaseURL:  mongodoc.BaseURL(url),
				Revision: rev,
			})
			if err != nil && !mgo.IsDup(err) {
				return errgo.Notef(err, "insert %v failed", url)
			}
			return nil
		})
	}
	if err := run.Wait(); err != nil {
		return errgo.Mask(err)
	}
	return nil
}
