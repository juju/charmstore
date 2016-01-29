// Copyright 2014 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package charmstore // import "gopkg.in/juju/charmstore.v5-unstable/internal/charmstore"

import (
	"gopkg.in/errgo.v1"
	"gopkg.in/juju/charm.v6-unstable"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"gopkg.in/juju/charmstore.v5-unstable/internal/mongodoc"
)

const (
	migrationAddSupportedSeries             mongodoc.MigrationName = "add supported series"
	migrationAddDevelopment                 mongodoc.MigrationName = "add development"
	migrationAddDevelopmentACLs             mongodoc.MigrationName = "add development acls"
	migrationFixBogusPromulgatedURL         mongodoc.MigrationName = "fix promulgate url"
	migrationAddEntityStable                mongodoc.MigrationName = "add entity stable flag"
	migrationChangeEntityDevelopment        mongodoc.MigrationName = "change entity development flag"
	migrationAddBaseEntityStableSeries      mongodoc.MigrationName = "add base entity stable series"
	migrationAddBaseEntityDevelopmentSeries mongodoc.MigrationName = "add base entity development series"
	migrationAddBaseEntityStableACLs        mongodoc.MigrationName = "add base entity stable acls"
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
	name:    migrationAddSupportedSeries,
	migrate: addSupportedSeries,
}, {
	name:    migrationAddDevelopment,
	migrate: addDevelopment,
}, {
	name:    migrationAddDevelopmentACLs,
	migrate: addDevelopmentACLs,
}, {
	name:    migrationFixBogusPromulgatedURL,
	migrate: fixBogusPromulgatedURL,
}, {
	name:    migrationAddEntityStable,
	migrate: addEntityStable,
}, {
	name:    migrationChangeEntityDevelopment,
	migrate: changeEntityDevelopment,
}, {
	name:    migrationAddBaseEntityStableSeries,
	migrate: addBaseEntityStableSeries,
}, {
	name:    migrationAddBaseEntityDevelopmentSeries,
	migrate: addBaseEntityDevelopmentSeries,
}, {
	name:    migrationAddBaseEntityStableACLs,
	migrate: addBaseEntityStableACLs,
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

// addSupportedSeries adds the supported-series field
// to entities that don't have it. Note that it does not
// need to work for multi-series charms because support
// for those has not been implemented before this migration.
func addSupportedSeries(db StoreDatabase) error {
	entities := db.Entities()
	var entity mongodoc.Entity
	iter := entities.Find(bson.D{{
		// Use the supportedseries field to collect not migrated entities.
		"supportedseries", bson.D{{"$exists", false}},
	}, {
		"series", bson.D{{"$ne", "bundle"}},
	}}).Select(bson.D{{"_id", 1}}).Iter()
	defer iter.Close()

	for iter.Next(&entity) {
		logger.Infof("updating %s", entity.URL)
		if err := entities.UpdateId(entity.URL, bson.D{{
			"$set", bson.D{
				{"supportedseries", []string{entity.URL.Series}},
			},
		}}); err != nil {
			return errgo.Notef(err, "cannot denormalize entity id %s", entity.URL)
		}
	}
	if err := iter.Close(); err != nil {
		return errgo.Notef(err, "cannot iterate entities")
	}
	return nil
}

// addDevelopment adds the Development field to all entities on which that
// field is not present.
func addDevelopment(db StoreDatabase) error {
	logger.Infof("adding development field to all entities")
	if _, err := db.Entities().UpdateAll(bson.D{{
		"development", bson.D{{"$exists", false}},
	}}, bson.D{{
		"$set", bson.D{{"development", false}},
	}}); err != nil {
		return errgo.Notef(err, "cannot add development field to all entities")
	}
	return nil
}

// addDevelopmentACLs sets up ACLs on base entities for development revisions.
func addDevelopmentACLs(db StoreDatabase) error {
	logger.Infof("adding development ACLs to all base entities")
	baseEntities := db.BaseEntities()
	var baseEntity mongodoc.BaseEntity
	iter := baseEntities.Find(bson.D{{
		"developmentacls", bson.D{{"$exists", false}},
	}}).Select(bson.D{{"_id", 1}, {"acls", 1}}).Iter()
	defer iter.Close()
	for iter.Next(&baseEntity) {
		if err := baseEntities.UpdateId(baseEntity.URL, bson.D{{
			"$set", bson.D{{"developmentacls", baseEntity.ACLs}},
		}}); err != nil {
			return errgo.Notef(err, "cannot add development ACLs to base entity id %s", baseEntity.URL)
		}
	}
	if err := iter.Close(); err != nil {
		return errgo.Notef(err, "cannot iterate base entities")
	}
	return nil
}

func fixBogusPromulgatedURL(db StoreDatabase) error {
	var entity mongodoc.Entity
	iter := db.Entities().Find(bson.D{{
		"promulgated-url", bson.D{{"$regex", "^cs:development/"}},
	}}).Select(map[string]int{
		"promulgated-url": 1,
	}).Iter()
	for iter.Next(&entity) {
		if entity.PromulgatedURL.Channel == "" {
			continue
		}
		entity.PromulgatedURL.Channel = ""
		if err := db.Entities().UpdateId(entity.URL, bson.D{{
			"$set", bson.D{{"promulgated-url", entity.PromulgatedURL}},
		}}); err != nil {
			return errgo.Notef(err, "cannot fix bogus promulgated URL for entity %v", entity.URL)
		}
	}
	if err := iter.Err(); err != nil {
		return errgo.Notef(err, "cannot iterate through entities")
	}
	return nil
}

func setExecuted(db StoreDatabase, name mongodoc.MigrationName) error {
	if _, err := db.Migrations().Upsert(nil, bson.D{{
		"$addToSet", bson.D{{"executed", name}},
	}}); err != nil {
		return errgo.Notef(err, "cannot add %s to executed migrations", name)
	}
	return nil
}

// addEntityStable adds the Stable field to all entities on which that
// field is not present. The field is set to true for all revisions on which
// the Development flag is false.
func addEntityStable(db StoreDatabase) error {
	logger.Infof("adding stable field to all entities")
	if _, err := db.Entities().UpdateAll(bson.D{{
		"stable", bson.D{{"$exists", false}},
	}}, bson.D{{
		"$set", bson.D{{"stable", false}},
	}}); err != nil {
		return errgo.Notef(err, "cannot add stable field to all entities")
	}
	logger.Infof("enabling the stable status for all non development entities")
	if _, err := db.Entities().UpdateAll(bson.D{{
		"development", false,
	}}, bson.D{{
		"$set", bson.D{{"stable", true}},
	}}); err != nil {
		return errgo.Notef(err, "cannot enable stable status")
	}
	return nil
}

// changeEntityDevelopment sets to true the Development flag for all entities.
// From now on, the Development flag indicates whether the revision is the
// current development release or has been a development release in the past.
func changeEntityDevelopment(db StoreDatabase) error {
	logger.Infof("changing development field to all entities")
	if _, err := db.Entities().UpdateAll(nil, bson.D{{
		"$set", bson.D{{"development", true}},
	}}); err != nil {
		return errgo.Notef(err, "cannot enable development status")
	}
	return nil
}

// addBaseEntityStableSeries adds the StableSeries field to all base entities
// on which that field is not present. This migration also populates the field
// with all the latest fully qualified URLs, for all existing series, having
// the Stable flag set to true.
func addBaseEntityStableSeries(db StoreDatabase) error {
	return addBaseEntitySeries(db, "stable")
}

// addBaseEntityDevelopmentSeries adds the DevelopmentSeries field to all base
// entities on which that field is not present. This migration also populates
// the field with all the latest fully qualified URLs, for all existing series.
func addBaseEntityDevelopmentSeries(db StoreDatabase) error {
	return addBaseEntitySeries(db, "development")
}

func addBaseEntitySeries(db StoreDatabase, channel string) error {
	logger.Infof("adding current %s series to all base entities", channel)
	baseEntities := db.BaseEntities()
	var baseEntity mongodoc.BaseEntity
	field := channel + "series"
	iter := baseEntities.Find(bson.D{{
		field, bson.D{{"$exists", false}},
	}}).Select(bson.D{{"_id", 1}}).Iter()
	defer iter.Close()
	for iter.Next(&baseEntity) {
		series, err := retrieveSeries(db, baseEntity.URL, channel)
		if err != nil {
			return errgo.Notef(err, "cannot collect entity series")
		}
		if err := baseEntities.UpdateId(baseEntity.URL, bson.D{{
			"$set", bson.D{{field, series}},
		}}); err != nil {
			return errgo.Notef(err, "cannot add %s series to base entity id %s", channel, baseEntity.URL)
		}
	}
	if err := iter.Close(); err != nil {
		return errgo.Notef(err, "cannot iterate base entities")
	}
	return nil
}

func retrieveSeries(db StoreDatabase, url *charm.URL, channel string) (map[string]*charm.URL, error) {
	allSeries := make(map[string]*charm.URL, len(seriesScore))
	for series := range seriesScore {
		if series == "" {
			continue
		}
		var entity mongodoc.Entity
		err := db.Entities().Find(
			bson.D{{"name", url.Name}, {"user", url.User}, {
				"$or", []bson.D{{{"supportedseries", series}}, {{"series", series}}},
			}, {channel, true}},
		).Select(bson.D{{"url", 1}}).Sort("-revision").Limit(1).One(&entity)
		if err == mgo.ErrNotFound {
			continue
		}
		if err != nil {
			return nil, errgo.Notef(err, "cannot query entities")
		}
		allSeries[series] = entity.URL
	}
	return allSeries, nil
}

// addBaseEntityStableACLs sets up StableACLs on base entities for stable
// revisions.
func addBaseEntityStableACLs(db StoreDatabase) error {
	logger.Infof("adding stable ACLs to all base entities")
	baseEntities := db.BaseEntities()
	var baseEntity mongodoc.BaseEntity
	iter := baseEntities.Find(bson.D{{
		"stableacls", bson.D{{"$exists", false}},
	}}).Select(bson.D{{"_id", 1}, {"acls", 1}, {"developmentacls", 1}}).Iter()
	defer iter.Close()
	for iter.Next(&baseEntity) {
		acls := baseEntity.ACLs
		if err := baseEntities.UpdateId(baseEntity.URL, bson.D{{
			"$set", bson.D{{"stableacls", acls}, {"acls", baseEntity.DevelopmentACLs}},
		}}); err != nil {
			return errgo.Notef(err, "cannot add stable ACLs to base entity id %s", baseEntity.URL)
		}
	}
	if err := iter.Close(); err != nil {
		return errgo.Notef(err, "cannot iterate base entities")
	}
	return nil
}
