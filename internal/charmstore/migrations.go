// Copyright 2014 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package charmstore // import "gopkg.in/juju/charmstore.v5-unstable/internal/charmstore"

import (
	"gopkg.in/errgo.v1"
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
	name:    migrationEdgeEntities,
	migrate: migrateEdgeEntities,
}, {
	name:    migrationEdgeBaseEntities,
	migrate: migrateEdgeBaseEntities,
}}

// migration holds a migration function with its corresponding name.
type migration struct {
	name    mongodoc.MigrationName
	migrate func(StoreDatabase) error
}

// Migrate starts the migration process using the given database.
func migrate(db StoreDatabase) error {
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

// migrateEdgeEntities renames the "development" entity field to "edge".
func migrateEdgeEntities(db StoreDatabase) error {
	if _, err := db.Entities().UpdateAll(nil, bson.D{{
		"$rename", bson.D{{"development", "edge"}},
	}}); err != nil {
		return errgo.Notef(err, "cannot rename development field in entities")
	}
	return nil
}

// migrateEdgeBaseEntities renames all "development" keys in base entity
// embedded documents to "edge".
func migrateEdgeBaseEntities(db StoreDatabase) error {
	if _, err := db.BaseEntities().UpdateAll(nil, bson.D{{
		"$rename", bson.D{
			{"channelacls.development", "channelacls.edge"},
			{"channelentities.development", "channelentities.edge"},
			{"channelresources.development", "channelresources.edge"},
		},
	}}); err != nil {
		return errgo.Notef(err, "cannot rename development keys in base entities")
	}
	return nil
}
