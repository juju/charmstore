package main

import (
	"log"
	"strconv"
	"strings"
	"time"

	errgo "gopkg.in/errgo.v1"
	"gopkg.in/juju/charmrepo.v3/csclient/params"
	"gopkg.in/juju/charmstore.v5/internal/charmstore"
	"gopkg.in/juju/charmstore.v5/internal/mongodoc"
	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

const (
	statCountersColl = "juju.stat.counters"
	statTokensColl   = "juju.stat.tokens"
	migrationsColl   = "migrations"
)

var keepKinds = []string{
	"archive-download",
	"archive-download-promulgated",
	"archive-delete",
	"archive-upload",
}

// squashStats combines entries in the stats counters
// into 1-hour granularity rather than 1-minute granularity.
func squashStats(db *mgo.Database) error {
	seconds := int(charmstore.StatsGranularity / time.Second)
	var r []struct{}
	err := db.C(statCountersColl).Pipe([]bson.M{{
		"$group": bson.M{
			"_id": bson.M{
				"t": bson.M{
					"$subtract": []interface{}{
						"$t",
						bson.M{
							"$mod": []interface{}{"$t", seconds},
						},
					},
				},
				"k": "$k",
			},
			"id": bson.M{
				"$first": "$_id",
			},
			"c": bson.M{
				"$sum": "$c",
			},
		},
	}, {
		"$project": bson.M{
			"_id": "$id",
			"c":   "$c",
			"k":   "$_id.k",
			"t":   "$_id.t",
		},
	}, {
		"$out": "newstatcounters",
	}}).AllowDiskUse().All(&r)
	if err != nil {
		return errgo.Notef(err, "cannot squash stats")
	}
	if err := renameCollection(db, "newstatcounters", statCountersColl); err != nil {
		return errgo.Mask(err)
	}
	return nil
}

func reorderStatsKeys(db *mgo.Database) error {
	const maxQueue = 250

	keepKeys := make(map[string]bool)
	for _, k := range keepKinds {
		key, err := statsKey(db, k)
		if err != nil {
			if errgo.Cause(err) == params.ErrNotFound {
				continue
			}
			return errgo.Newf("cannot determine key for %q: %v", k, err)
		}
		keepKeys[key] = true
	}
	keepKeysSlice := make([]string, 0, len(keepKeys))
	for k := range keepKeys {
		keepKeysSlice = append(keepKeysSlice, k)
	}

	counters := db.C(statCountersColl)
	iter := counters.Find(bson.M{
		"k": bson.M{
			"$regex": "^(" + strings.Join(keepKeysSlice, "|") + "):",
		},
	}).Batch(500).Prefetch(0.5).Iter()
	defer iter.Close()
	newCounters := db.C("newstatcounters")

	inserter := newCounters.Bulk()
	inserter.Unordered()
	queued := 0
	discarded := 0
	count := 0
	t0 := time.Now()
	tsec := time.Duration(0)
	flush := func() {
		if _, err := inserter.Run(); err != nil {
			log.Printf("cannot insert %d docs: %v", queued, err)
		}
		queued = 0
		inserter = newCounters.Bulk()
		inserter.Unordered()
		if tsec1 := time.Since(t0).Truncate(10 * time.Second); tsec1 != tsec {
			tsec = tsec1
			logger.Infof("flush %d; discarded %d", count, discarded)
		}
	}
	for {
		var doc struct {
			Id    bson.ObjectId `bson:"_id"`
			Key   string        `bson:"k"`
			Time  int64         `bson:"t"`
			Count int64         `bson:"c"`
		}
		if !iter.Next(&doc) {
			break
		}
		count++
		fields := strings.Split(doc.Key, ":")
		if len(fields) != 6 && len(fields) != 5 {
			discarded++
			continue
		}
		if len(fields) == 6 {
			doc.Key = "" +
				fields[0] + ":" + // kind
				fields[3] + ":" + // user
				fields[2] + ":" + // name
				fields[1] + ":" + // series
				fields[4] + ":" // revision
		} else {
			doc.Key = "" +
				fields[0] + ":" + // kind
				fields[3] + ":" + // user
				fields[2] + ":" + // name
				fields[1] + ":" // series
		}
		inserter.Insert(&doc)
		queued++
		if queued >= maxQueue {
			flush()
		}
	}
	flush()
	if err := iter.Err(); err != nil {
		return errgo.Notef(err, "iteration failed")
	}
	logger.Infof("migrated keys: %d copied; %d discarded", count, discarded)
	logger.Infof("creating index")
	if err := newCounters.EnsureIndex(mgo.Index{
		Key:    []string{"k", "t"},
		Unique: true,
	}); err != nil {
		return errgo.Notef(err, "ensure index failed")
	}
	logger.Infof("renaming counters collection")
	if err := renameCollection(db, "newstatcounters", statCountersColl); err != nil {
		return errgo.Mask(err)
	}
	return nil
}

func renameCollection(db *mgo.Database, old, new string) error {
	if err := db.Session.Run(bson.D{
		{"renameCollection", db.Name + "." + old},
		{"to", db.Name + "." + new},
		{"dropTarget", true},
	}, &struct{}{}); err != nil {
		return errgo.Notef(err, "cannot rename collection")
	}
	return nil
}

// Note: the following code is largely copied from charmstore/migrations.go

func getExecutedMigrations(db *mgo.Database) (map[mongodoc.MigrationName]bool, error) {
	// Retrieve the already executed migration names.
	var doc mongodoc.Migration
	if err := db.C(migrationsColl).Find(nil).Select(bson.D{{"executed", 1}}).One(&doc); err != nil {
		if err != mgo.ErrNotFound {
			return nil, errgo.Notef(err, "cannot retrieve executed migrations")
		}
	}
	executed := make(map[mongodoc.MigrationName]bool)
	for _, name := range doc.Executed {
		executed[mongodoc.MigrationName(name)] = true
	}
	return executed, nil
}

func setExecutedMigration(db *mgo.Database, name mongodoc.MigrationName) error {
	if _, err := db.C(migrationsColl).Upsert(nil, bson.D{{
		"$addToSet", bson.D{{"executed", name}},
	}}); err != nil {
		return errgo.Notef(err, "cannot add %s to executed migrations", name)
	}
	return nil
}

type tokenId struct {
	Id    int    `bson:"_id"`
	Token string `bson:"t"`
}

func statsKey(db *mgo.Database, key string) (string, error) {
	tokens := db.C(statTokensColl)
	var t tokenId
	err := tokens.Find(bson.D{{"t", key}}).One(&t)
	if err != nil {
		return "", errgo.Mask(err, errgo.Is(mgo.ErrNotFound))
	}
	return strconv.FormatInt(int64(t.Id), 32), nil
}
