package charmstore

import (
	"flag"
	"net/http"
	"sort"

	jujutesting "github.com/juju/testing"
	jc "github.com/juju/testing/checkers"
	gc "gopkg.in/check.v1"
	"gopkg.in/errgo.v1"
	"gopkg.in/juju/charm.v6"
	"gopkg.in/juju/charmrepo.v3/csclient/params"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"gopkg.in/juju/charmstore.v5/internal/mongodoc"
	"gopkg.in/juju/charmstore.v5/internal/storetesting"
)

type migrationsIntegrationSuite struct {
	commonSuite
}

var _ = gc.Suite(&migrationsIntegrationSuite{})

const earliestDeployedVersion = "4.5.9"

// To create a dump file, run:
// 	go test -check.f migrationsIntegrationSuite -dump-migration-history
var dumpMigrationHistoryFlag = flag.Bool("dump-migration-history", false, "dump migration history to file")

func (s *migrationsIntegrationSuite) SetUpSuite(c *gc.C) {
	// Make sure logging works even before the rest of
	// commonSuite is started.
	s.LoggingSuite.SetUpSuite(c)
	if *dumpMigrationHistoryFlag {
		s.dump(c)
	}
	s.commonSuite.SetUpSuite(c)
}

func (s *migrationsIntegrationSuite) dump(c *gc.C) {
	// We can't use the usual s.Session because we're using
	// commonSuite which uses IsolationSuite which hides the
	// environment variables which are needed for
	// dumpMigrationHistory to run.
	session, err := jujutesting.MgoServer.Dial()
	c.Assert(err, gc.Equals, nil)
	defer session.Close()
	err = dumpMigrationHistory(session, earliestDeployedVersion, migrationHistory)
	c.Assert(err, gc.Equals, nil)
}

var migrationHistory = []versionSpec{{
	version: "4.1.5",
	pkg:     "gopkg.in/juju/charmstore.v5-unstable",
	update: func(db *mgo.Database, csv *charmStoreVersion) error {
		err := csv.Upload("v4", []uploadSpec{{
			id:            "~charmers/precise/promulgated-0",
			promulgatedId: "precise/promulgated-0",
			entity:        storetesting.NewCharm(nil),
		}, {
			id:     "~bob/trusty/nonpromulgated-0",
			entity: storetesting.NewCharm(nil),
		}, {
			id:            "~charmers/bundle/promulgatedbundle-0",
			promulgatedId: "bundle/promulgatedbundle-0",
			entity: storetesting.NewBlob([]storetesting.File{{
				Name: "README.md",
				Data: []byte("something"),
			}, {
				Name: "bundle.yaml",
				Data: []byte(`services: {promulgated: {charm: promulgated}}`),
			}}),
		}, {
			id: "~charmers/bundle/nonpromulgatedbundle-0",
			entity: storetesting.NewBlob([]storetesting.File{{
				Name: "README.md",
				Data: []byte("something"),
			}, {
				Name: "bundle.yaml",
				Data: []byte(`services: {promulgated: {charm: promulgated}}`),
			}}),
		}})
		if err != nil {
			return errgo.Mask(err)
		}
		if err := csv.Put("/v4/~charmers/precise/promulgated/meta/perm", params.PermRequest{
			Read:  []string{"everyone"},
			Write: []string{"alice", "bob", "charmers"},
		}); err != nil {
			return errgo.Mask(err)
		}
		if err := csv.Put("/v4/~bob/trusty/nonpromulgated/meta/perm", params.PermRequest{
			Read:  []string{"bobgroup"},
			Write: []string{"bob", "someoneelse"},
		}); err != nil {
			return errgo.Mask(err)
		}

		return nil
	},
}, {
	// Multi-series charms.
	// Development channel + ACLs
	version: "4.3.0",
	pkg:     "gopkg.in/juju/charmstore.v5-unstable",
	update: func(db *mgo.Database, csv *charmStoreVersion) error {
		err := csv.Upload("v4", []uploadSpec{{
			// Uploads to ~charmers/multiseries-0
			id: "~charmers/multiseries",
			//  Note: PUT doesn't work on multi-series.
			usePost: true,
			entity: storetesting.NewCharm(&charm.Meta{
				Series: []string{"precise", "trusty", "utopic"},
			}),
		}, {
			// This triggers the bug where we created a base
			// entity with a bogus "development" channel in the URL.
			// Uploads to ~charmers/precise/promulgated-1
			id:      "~charmers/development/precise/promulgated",
			usePost: true,
			entity: storetesting.NewCharm(&charm.Meta{
				Name: "different",
			}),
		}})
		if err != nil {
			return errgo.Mask(err)
		}

		// Sanity check that we really did trigger the bug.
		err = db.C("entities").Find(bson.D{{
			"promulgated-url", "cs:development/precise/promulgated-1",
		}}).One(new(interface{}))
		if err != nil {
			return errgo.Notef(err, "we don't seem to have triggered the bug")
		}

		if err := csv.Put("/v4/development/promulgated/meta/perm", params.PermRequest{
			Read:  []string{"charmers"},
			Write: []string{"charmers"},
		}); err != nil {
			return errgo.Mask(err)
		}
		return nil
	},
}, {
	// V5 API.
	// Fix bogus promulgated URL.
	// V4 multi-series compatibility (this didn't work).
	version: "4.4.3",
	pkg:     "gopkg.in/juju/charmstore.v5-unstable",
	update: func(db *mgo.Database, csv *charmStoreVersion) error {
		err := csv.Upload("v5", []uploadSpec{{
			// Uploads to ~charmers/multiseries-1
			id:      "~charmers/multiseries",
			usePost: true,
			entity: storetesting.NewCharm(&charm.Meta{
				Series: []string{"precise", "trusty", "wily"},
			}),
		}, {
			id:     "~someone/precise/southerncharm-0",
			entity: storetesting.NewCharm(nil),
		}, {
			id:     "~someone/development/precise/southerncharm-3",
			entity: storetesting.NewCharm(nil),
		}, {
			id:     "~someone/development/trusty/southerncharm-5",
			entity: storetesting.NewCharm(nil),
		}, {
			id:     "~someone/trusty/southerncharm-6",
			entity: storetesting.NewCharm(nil),
		}})
		if err != nil {
			return errgo.Mask(err)
		}
		return nil
	},
}, {
	// V5 API.
	// Copy from extra-info/legacy-download-stats to Archive Downloads.
	// Create Charm 1 with 3 revisions set extrainfo legacy download stats on number 3
	// Create Charm 2 with 3 revisions set extrainfo legacy download stats on number 2
	// Create Charm 3 with 1 revision set extrainfo legacy download stats on it
	// Create Charm 4 with 3 revisions no extrainfo legacy download stats
	// Create Charm 5 with 1 revisions no extrainfo legacy download stats
	// Check the results in increase by 10 for all revision when legacy is set.
	version: "4.5.3",
	pkg:     "gopkg.in/juju/charmstore.v5-unstable",
	update: func(db *mgo.Database, csv *charmStoreVersion) error {
		err := csv.Upload("v5", []uploadSpec{{
			id:     "~charmers/trusty/legacystats-setonlast-0",
			entity: storetesting.NewCharm(nil),
		}, {
			id:     "~charmers/trusty/legacystats-setonlast-1",
			entity: storetesting.NewCharm(nil),
		}, {
			id:     "~charmers/trusty/legacystats-setonlast-2",
			entity: storetesting.NewCharm(nil),
		}, {
			id:     "~charmers/trusty/legacystats-setonsecond-0",
			entity: storetesting.NewCharm(nil),
		}, {
			id:     "~charmers/trusty/legacystats-setonsecond-1",
			entity: storetesting.NewCharm(nil),
		}, {
			id:     "~charmers/trusty/legacystats-setonsecond-2",
			entity: storetesting.NewCharm(nil),
		}, {
			id:     "~charmers/trusty/legacystats-setonfirst-0",
			entity: storetesting.NewCharm(nil),
		}, {
			id:     "~charmers/trusty/legacystats-setonfirst-1",
			entity: storetesting.NewCharm(nil),
		}, {
			id:     "~charmers/trusty/legacystats-setonfirst-2",
			entity: storetesting.NewCharm(nil),
		}, {
			id:     "~charmers/trusty/legacystats-2rev-notset-0",
			entity: storetesting.NewCharm(nil),
		}, {
			id:     "~charmers/trusty/legacystats-2rev-notset-1",
			entity: storetesting.NewCharm(nil),
		}, {
			id:     "~charmers/trusty/legacystats-1rev-notset-0",
			entity: storetesting.NewCharm(nil),
		}, {
			id:     "~someone/trusty/empty-metered-42",
			entity: storetesting.NewCharm(nil).WithMetrics(&charm.Metrics{}),
		}})

		if err != nil {
			return errgo.Mask(err)
		}

		if err := csv.Put("/v5/~charmers/trusty/legacystats-setonlast-2/meta/extra-info/legacy-download-stats", 10); err != nil {
			return errgo.Mask(err)
		}
		if err := csv.Put("/v5/~charmers/trusty/legacystats-setonsecond-1/meta/extra-info/legacy-download-stats", 100); err != nil {
			return errgo.Mask(err)
		}
		if err := csv.Put("/v5/~charmers/trusty/legacystats-setonfirst-0/meta/extra-info/legacy-download-stats", 1000); err != nil {
			return errgo.Mask(err)
		}
		return nil
	},
}, {
	// V5 API.
	// Rename the development channel to "edge", in both entities and base
	// entities.
	// Deletes the "edge" and "stable" boolean fields in the entity document
	// and replace them with a single "published" map.
	// Populate base entity ACLs for the candidate and beta channels.
	version: "4.5.6",
	pkg:     "gopkg.in/juju/charmstore.v5-unstable",
	update: func(db *mgo.Database, csv *charmStoreVersion) error {
		err := csv.Upload("v5", []uploadSpec{{
			id:     "~charmers/trusty/different-acls-0",
			entity: storetesting.NewCharm(nil),
		}})
		if err != nil {
			return errgo.Mask(err)
		}
		url := charm.MustParseURL("~charmers/different-acls")
		err = db.C("base_entities").UpdateId(url, bson.D{{
			"$set", bson.D{
				{"channelacls.unpublished", mongodoc.ACL{
					Read:  []string{"everyone", "unpublished"},
					Write: []string{"everyone", "charmers", "unpublished"},
				}},
				{"channelacls.development", mongodoc.ACL{
					Read:  []string{"everyone", "edge"},
					Write: []string{"everyone", "charmers", "edge"},
				}},
			},
		}})
		if err != nil {
			return errgo.Notef(err, "cannot update ACLs for base entity %q", url)
		}
		return nil
	},
}, {
	// Add support for new channels: stable, candidate, beta and edge.
	// Add zesty to series.
	version: "4.5.9",
	pkg:     "gopkg.in/juju/charmstore.v5-unstable",
	update: func(db *mgo.Database, csv *charmStoreVersion) error {
		// TODO add charm that's published to new channels.
		err := csv.Upload("v5", []uploadSpec{{
			usePost: true,
			// Uploads to ~charmers/zesty/promulgated-0
			id: "~charmers/zesty/promulgated",
			// Uploads to zesty/promulgated-0
			promulgatedId: "zesty/promulgated",
			entity:        storetesting.NewCharm(nil),
		}, {
			usePost: true,
			// Uploads to ~charmers/allchans-0
			id: "~charmers/allchans",
			entity: storetesting.NewCharm(&charm.Meta{
				Series: []string{"xenial"},
			}),
		}})
		if err != nil {
			return errgo.Mask(err)
		}
		if err := csv.Put("/v5/~charmers/allchans-0/publish", &params.PublishRequest{
			Channels: []params.Channel{
				params.StableChannel,
				params.CandidateChannel,
				params.BetaChannel,
				params.EdgeChannel,
			},
		}); err != nil {
			return errgo.Mask(err)
		}
		return nil
	},
}}

var migrationFromDumpEntityTests = []struct {
	id       string
	checkers []entityChecker
}{{
	id: "~charmers/precise/promulgated-0",
	checkers: []entityChecker{
		hasPromulgatedRevision(0),
		hasCompatibilityBlob(false),
		isPublished(params.EdgeChannel, params.StableChannel),
	},
}, {
	id: "~charmers/precise/promulgated-1",
	checkers: []entityChecker{
		hasPromulgatedRevision(1),
		hasCompatibilityBlob(false),
		isPublished(params.EdgeChannel),
	},
}, {
	id: "~charmers/zesty/promulgated-0",
	checkers: []entityChecker{
		hasPromulgatedRevision(0),
		hasCompatibilityBlob(false),
		isPublished(),
	},
}, {
	id: "~bob/trusty/nonpromulgated-0",
	checkers: []entityChecker{
		hasPromulgatedRevision(-1),
		hasCompatibilityBlob(false),
		isPublished(params.EdgeChannel, params.StableChannel),
	},
}, {
	id: "~charmers/bundle/promulgatedbundle-0",
	checkers: []entityChecker{
		hasPromulgatedRevision(0),
		hasCompatibilityBlob(false),
		isPublished(params.EdgeChannel, params.StableChannel),
	},
}, {
	id: "~charmers/bundle/nonpromulgatedbundle-0",
	checkers: []entityChecker{
		hasPromulgatedRevision(-1),
		hasCompatibilityBlob(false),
		isPublished(params.EdgeChannel, params.StableChannel),
	},
}, {
	id: "~charmers/multiseries-0",
	checkers: []entityChecker{
		hasPromulgatedRevision(-1),
		hasCompatibilityBlob(true),
		isPublished(params.EdgeChannel, params.StableChannel),
	},
}, {
	id: "~charmers/multiseries-1",
	checkers: []entityChecker{
		hasPromulgatedRevision(-1),
		hasCompatibilityBlob(true),
		isPublished(params.EdgeChannel, params.StableChannel),
	},
}, {
	id: "~someone/precise/southerncharm-0",
	checkers: []entityChecker{
		hasPromulgatedRevision(-1),
		hasCompatibilityBlob(false),
		isPublished(params.EdgeChannel, params.StableChannel),
	},
}, {
	id: "~someone/precise/southerncharm-3",
	checkers: []entityChecker{
		hasPromulgatedRevision(-1),
		hasCompatibilityBlob(false),
		isPublished(params.EdgeChannel),
	},
}, {
	id: "~someone/trusty/southerncharm-5",
	checkers: []entityChecker{
		hasPromulgatedRevision(-1),
		hasCompatibilityBlob(false),
		isPublished(params.EdgeChannel),
	},
}, {
	id: "~someone/trusty/southerncharm-6",
	checkers: []entityChecker{
		hasPromulgatedRevision(-1),
		hasCompatibilityBlob(false),
		isPublished(params.EdgeChannel, params.StableChannel),
		hasMetrics(nil),
	},
}, {
	id: "~someone/trusty/empty-metered-42",
	checkers: []entityChecker{
		hasMetrics(nil),
		hasPromulgatedRevision(-1),
	},
}, {
	id: "~charmers/allchans-0",
	checkers: []entityChecker{
		isPublished(
			params.StableChannel,
			params.CandidateChannel,
			params.BetaChannel,
			params.EdgeChannel,
		),
	},
}}

var migrationFromDumpBaseEntityTests = []struct {
	id       string
	checkers []baseEntityChecker
}{{
	id: "cs:~charmers/promulgated",
	checkers: []baseEntityChecker{
		isPromulgated(true),
		hasACLs(map[params.Channel]mongodoc.ACL{
			params.UnpublishedChannel: {
				Read:  []string{"charmers"},
				Write: []string{"charmers"},
			},
			params.EdgeChannel: {
				Read:  []string{"charmers"},
				Write: []string{"charmers"},
			},
			params.BetaChannel: {
				Read:  []string{"charmers"},
				Write: []string{"charmers"},
			},
			params.CandidateChannel: {
				Read:  []string{"charmers"},
				Write: []string{"charmers"},
			},
			params.StableChannel: {
				Read:  []string{"everyone"},
				Write: []string{"alice", "bob", "charmers"},
			},
		}),
		hasChannelEntities(map[params.Channel]map[string]*charm.URL{
			params.EdgeChannel: {
				"precise": charm.MustParseURL("~charmers/precise/promulgated-1"),
			},
			params.StableChannel: {
				"precise": charm.MustParseURL("~charmers/precise/promulgated-0"),
			},
		}),
	},
}, {
	id: "cs:~bob/nonpromulgated",
	checkers: []baseEntityChecker{
		isPromulgated(false),
		hasACLs(map[params.Channel]mongodoc.ACL{
			params.UnpublishedChannel: {
				Read:  []string{"bobgroup"},
				Write: []string{"bob", "someoneelse"},
			},
			params.EdgeChannel: {
				Read:  []string{"bobgroup"},
				Write: []string{"bob", "someoneelse"},
			},
			params.BetaChannel: {
				Read:  []string{"bobgroup"},
				Write: []string{"bob", "someoneelse"},
			},
			params.CandidateChannel: {
				Read:  []string{"bobgroup"},
				Write: []string{"bob", "someoneelse"},
			},
			params.StableChannel: {
				Read:  []string{"bobgroup"},
				Write: []string{"bob", "someoneelse"},
			},
		}),
		hasChannelEntities(map[params.Channel]map[string]*charm.URL{
			params.EdgeChannel: {
				"trusty": charm.MustParseURL("~bob/trusty/nonpromulgated-0"),
			},
			params.StableChannel: {
				"trusty": charm.MustParseURL("~bob/trusty/nonpromulgated-0"),
			},
		}),
	},
}, {
	id: "~charmers/promulgatedbundle",
	checkers: []baseEntityChecker{
		isPromulgated(true),
		hasAllACLs("charmers"),
		hasChannelEntities(map[params.Channel]map[string]*charm.URL{
			params.EdgeChannel: {
				"bundle": charm.MustParseURL("~charmers/bundle/promulgatedbundle-0"),
			},
			params.StableChannel: {
				"bundle": charm.MustParseURL("~charmers/bundle/promulgatedbundle-0"),
			},
		}),
	},
}, {
	id: "cs:~charmers/nonpromulgatedbundle",
	checkers: []baseEntityChecker{
		isPromulgated(false),
		hasAllACLs("charmers"),
		hasChannelEntities(map[params.Channel]map[string]*charm.URL{
			params.EdgeChannel: {
				"bundle": charm.MustParseURL("~charmers/bundle/nonpromulgatedbundle-0"),
			},
			params.StableChannel: {
				"bundle": charm.MustParseURL("~charmers/bundle/nonpromulgatedbundle-0"),
			},
		}),
	},
}, {
	id: "cs:~charmers/multiseries",
	checkers: []baseEntityChecker{
		isPromulgated(false),
		hasAllACLs("charmers"),
		hasChannelEntities(map[params.Channel]map[string]*charm.URL{
			params.EdgeChannel: {
				"precise": charm.MustParseURL("~charmers/multiseries-1"),
				"trusty":  charm.MustParseURL("~charmers/multiseries-1"),
				"utopic":  charm.MustParseURL("~charmers/multiseries-0"),
				"wily":    charm.MustParseURL("~charmers/multiseries-1"),
			},
			params.StableChannel: {
				"precise": charm.MustParseURL("~charmers/multiseries-1"),
				"trusty":  charm.MustParseURL("~charmers/multiseries-1"),
				"utopic":  charm.MustParseURL("~charmers/multiseries-0"),
				"wily":    charm.MustParseURL("~charmers/multiseries-1"),
			},
		}),
	},
}, {
	id: "cs:~someone/southerncharm",
	checkers: []baseEntityChecker{
		isPromulgated(false),
		hasAllACLs("someone"),
		hasChannelEntities(map[params.Channel]map[string]*charm.URL{
			params.EdgeChannel: {
				"precise": charm.MustParseURL("~someone/precise/southerncharm-3"),
				"trusty":  charm.MustParseURL("~someone/trusty/southerncharm-6"),
			},
			params.StableChannel: {
				"precise": charm.MustParseURL("~someone/precise/southerncharm-0"),
				"trusty":  charm.MustParseURL("~someone/trusty/southerncharm-6"),
			},
		}),
	},
}, {
	id: "cs:~charmers/different-acls",
	checkers: []baseEntityChecker{
		hasACLs(map[params.Channel]mongodoc.ACL{
			params.UnpublishedChannel: {
				Read:  []string{"everyone", "unpublished"},
				Write: []string{"everyone", "charmers", "unpublished"},
			},
			params.EdgeChannel: {
				Read:  []string{"everyone", "edge"},
				Write: []string{"everyone", "charmers", "edge"},
			},
			params.BetaChannel: {
				Read:  []string{"everyone", "unpublished"},
				Write: []string{"everyone", "charmers", "unpublished"},
			},
			params.CandidateChannel: {
				Read:  []string{"everyone", "unpublished"},
				Write: []string{"everyone", "charmers", "unpublished"},
			},
			params.StableChannel: {
				Read:  []string{"charmers"},
				Write: []string{"charmers"},
			},
		}),
	},
}}

func (s *migrationsIntegrationSuite) TestMigrationFromDump(c *gc.C) {
	db := s.Session.DB("juju_test")
	err := createDatabaseAtVersion(db, migrationHistory[len(migrationHistory)-1].version)
	c.Assert(err, gc.Equals, nil)
	err = s.runMigrations(db)
	c.Assert(err, gc.Equals, nil)

	store := s.newStore(c, false)
	defer store.Close()

	checkAllEntityInvariants(c, store)

	for i, test := range migrationFromDumpEntityTests {
		c.Logf("test %d: entity %v", i, test.id)

		e, err := store.FindEntity(MustParseResolvedURL(test.id), nil)
		c.Assert(err, gc.Equals, nil)
		for j, check := range test.checkers {
			c.Logf("test %d: entity %v; check %d", i, test.id, j)
			check(c, e)
		}
	}

	for i, test := range migrationFromDumpBaseEntityTests {
		c.Logf("test %d: base entity %v", i, test.id)

		e, err := store.FindBaseEntity(charm.MustParseURL(test.id), nil)
		c.Assert(err, gc.Equals, nil)
		for j, check := range test.checkers {
			c.Logf("test %d: base entity %v; check %d", i, test.id, j)
			check(c, e)
		}
	}

	// Check that the latest revisions table has been populated correctly.
	var revs []mongodoc.LatestRevision
	err = store.DB.Revisions().Find(nil).Sort("_id").All(&revs)
	c.Assert(err, gc.Equals, nil)

	expectRevisions := []string{
		"cs:bundle/promulgatedbundle-0",
		"cs:precise/promulgated-1",
		"cs:zesty/promulgated-0",
		"cs:~bob/trusty/nonpromulgated-0",
		"cs:~charmers/allchans-0",
		"cs:~charmers/bundle/nonpromulgatedbundle-0",
		"cs:~charmers/bundle/promulgatedbundle-0",
		"cs:~charmers/multiseries-1",
		"cs:~charmers/precise/promulgated-1",
		"cs:~charmers/trusty/different-acls-0",
		"cs:~charmers/trusty/legacystats-1rev-notset-0",
		"cs:~charmers/trusty/legacystats-2rev-notset-1",
		"cs:~charmers/trusty/legacystats-setonfirst-2",
		"cs:~charmers/trusty/legacystats-setonlast-2",
		"cs:~charmers/trusty/legacystats-setonsecond-2",
		"cs:~charmers/zesty/promulgated-0",
		"cs:~someone/precise/southerncharm-3",
		"cs:~someone/trusty/empty-metered-42",
		"cs:~someone/trusty/southerncharm-6",
	}
	sort.Strings(expectRevisions)
	expectRevDocs := make([]mongodoc.LatestRevision, len(expectRevisions))
	for i, r := range expectRevisions {
		url := charm.MustParseURL(r)
		expectRevDocs[i] = mongodoc.LatestRevision{
			URL:      url.WithRevision(-1),
			BaseURL:  mongodoc.BaseURL(url),
			Revision: url.Revision,
		}
	}
	c.Assert(revs, jc.DeepEquals, expectRevDocs)
}

func checkAllEntityInvariants(c *gc.C, store *Store) {
	var entities []*mongodoc.Entity

	err := store.DB.Entities().Find(nil).All(&entities)
	c.Assert(err, gc.Equals, nil)
	for _, e := range entities {
		c.Logf("check entity invariants %v", e.URL)
		checkEntityInvariants(c, e, store)
	}

	var baseEntities []*mongodoc.BaseEntity
	err = store.DB.BaseEntities().Find(nil).All(&baseEntities)
	c.Assert(err, gc.Equals, nil)
	for _, e := range baseEntities {
		c.Logf("check base entity invariants %v", e.URL)
		checkBaseEntityInvariants(c, e, store)
	}
}

func checkEntityInvariants(c *gc.C, e *mongodoc.Entity, store *Store) {
	// Basic "this must have some non-zero value" checks.
	c.Assert(e.URL.Name, gc.Not(gc.Equals), "")
	c.Assert(e.URL.Revision, gc.Not(gc.Equals), -1)
	c.Assert(e.URL.User, gc.Not(gc.Equals), "")

	c.Assert(e.PreV5BlobHash, gc.Not(gc.Equals), "")
	c.Assert(e.PreV5BlobHash256, gc.Not(gc.Equals), "")
	c.Assert(e.BlobHash, gc.Not(gc.Equals), "")
	c.Assert(e.BlobHash256, gc.Not(gc.Equals), "")
	c.Assert(e.Size, gc.Not(gc.Equals), 0)

	if e.UploadTime.IsZero() {
		c.Fatalf("zero upload time")
	}

	// URL denormalization checks.
	c.Assert(e.BaseURL, jc.DeepEquals, mongodoc.BaseURL(e.URL))
	c.Assert(e.URL.Name, gc.Equals, e.Name)
	c.Assert(e.URL.User, gc.Equals, e.User)
	c.Assert(e.URL.Revision, gc.Equals, e.Revision)
	c.Assert(e.URL.Series, gc.Equals, e.Series)

	if e.PromulgatedRevision != -1 {
		expect := *e.URL
		expect.User = ""
		expect.Revision = e.PromulgatedRevision
		c.Assert(e.PromulgatedURL, jc.DeepEquals, &expect)
	} else {
		c.Assert(e.PromulgatedURL, gc.IsNil)
	}

	// Multi-series vs single-series vs bundle checks.
	if e.URL.Series == "bundle" {
		c.Assert(e.BundleData, gc.NotNil)
		c.Assert(e.BundleCharms, gc.NotNil)
		c.Assert(e.BundleMachineCount, gc.NotNil)
		c.Assert(e.BundleUnitCount, gc.NotNil)

		c.Assert(e.SupportedSeries, gc.HasLen, 0)
		c.Assert(e.BlobHash, gc.Equals, e.PreV5BlobHash)
		c.Assert(e.Size, gc.Equals, e.PreV5BlobSize)
		c.Assert(e.BlobHash256, gc.Equals, e.PreV5BlobHash256)
	} else {
		c.Assert(e.CharmMeta, gc.NotNil)
		if e.URL.Series == "" {
			c.Assert(e.SupportedSeries, jc.DeepEquals, e.CharmMeta.Series)
			c.Assert(e.BlobHash, gc.Not(gc.Equals), e.PreV5BlobHash)
			c.Assert(e.Size, gc.Not(gc.Equals), e.PreV5BlobSize)
			c.Assert(e.BlobHash256, gc.Not(gc.Equals), e.PreV5BlobHash256)
		} else {
			c.Assert(e.SupportedSeries, jc.DeepEquals, []string{e.URL.Series})
			c.Assert(e.BlobHash, gc.Equals, e.PreV5BlobHash)
			c.Assert(e.Size, gc.Equals, e.PreV5BlobSize)
			c.Assert(e.BlobHash256, gc.Equals, e.PreV5BlobHash256)
		}
	}

	// Check that the blobs exist.
	r, err := store.OpenBlob(EntityResolvedURL(e))
	c.Assert(err, gc.Equals, nil)
	r.Close()
	r, err = store.OpenBlobPreV5(EntityResolvedURL(e))
	c.Assert(err, gc.Equals, nil)
	r.Close()

	// Check that the base entity exists.
	_, err = store.FindBaseEntity(e.URL, nil)
	c.Assert(err, gc.Equals, nil)
}

func stringInSlice(s string, ss []string) bool {
	for _, t := range ss {
		if s == t {
			return true
		}
	}
	return false
}

func checkBaseEntityInvariants(c *gc.C, e *mongodoc.BaseEntity, store *Store) {
	c.Assert(e.URL.Name, gc.Not(gc.Equals), "")
	c.Assert(e.URL.User, gc.Not(gc.Equals), "")

	c.Assert(e.URL, jc.DeepEquals, mongodoc.BaseURL(e.URL))
	c.Assert(e.User, gc.Equals, e.URL.User)
	c.Assert(e.Name, gc.Equals, e.URL.Name)

	// Check that each entity mentioned in ChannelEntities exists and has the
	// correct channel.
	for ch, seriesEntities := range e.ChannelEntities {
		c.Assert(ch, gc.Not(gc.Equals), params.UnpublishedChannel)
		for series, url := range seriesEntities {
			if url.Series != "" {
				c.Assert(url.Series, gc.Equals, series)
			}
			ce, err := store.FindEntity(MustParseResolvedURL(url.String()), nil)
			c.Assert(err, gc.Equals, nil)
			if !params.ValidChannels[ch] {
				c.Fatalf("unknown channel %q found", ch)
			}
			c.Assert(ce.Published[ch], gc.Equals, true)
			if series != "bundle" && !stringInSlice(series, ce.SupportedSeries) {
				c.Fatalf("series %q not found in supported series %q", series, ce.SupportedSeries)
			}
		}
	}
}

// runMigrations starts a new server which will cause all migrations
// to be triggered.
func (s *migrationsIntegrationSuite) runMigrations(db *mgo.Database) error {
	apiHandler := func(APIHandlerParams) (HTTPCloseHandler, error) {
		return nopCloseHandler{http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {})}, nil
	}
	srv, err := NewServer(db, nil, serverParams, map[string]NewAPIHandlerFunc{
		"version1": apiHandler,
	})
	if err == nil {
		srv.Close()
	}
	return err
}

type entityChecker func(c *gc.C, entity *mongodoc.Entity)

func hasPromulgatedRevision(rev int) entityChecker {
	return func(c *gc.C, entity *mongodoc.Entity) {
		c.Assert(entity.PromulgatedRevision, gc.Equals, rev)
	}
}

func hasCompatibilityBlob(hasBlob bool) entityChecker {
	return func(c *gc.C, entity *mongodoc.Entity) {
		if hasBlob {
			c.Assert(entity.PreV5BlobHash, gc.Not(gc.Equals), entity.BlobHash)
		} else {
			c.Assert(entity.PreV5BlobHash, gc.Equals, entity.BlobHash)
		}
	}
}

func isPublished(channels ...params.Channel) entityChecker {
	cmap := make(map[params.Channel]bool)
	for _, c := range channels {
		cmap[c] = true
	}
	return func(c *gc.C, entity *mongodoc.Entity) {
		for _, ch := range params.OrderedChannels {
			c.Assert(entity.Published[ch], gc.Equals, cmap[ch], gc.Commentf("channel %v", ch))
		}
	}
}

func hasMetrics(metrics *charm.Metrics) entityChecker {
	return func(c *gc.C, entity *mongodoc.Entity) {
		c.Assert(entity.CharmMetrics, jc.DeepEquals, metrics)
	}
}

type baseEntityChecker func(c *gc.C, entity *mongodoc.BaseEntity)

func isPromulgated(isProm bool) baseEntityChecker {
	return func(c *gc.C, entity *mongodoc.BaseEntity) {
		c.Assert(entity.Promulgated, gc.Equals, mongodoc.IntBool(isProm))
	}
}

func hasACLs(acls map[params.Channel]mongodoc.ACL) baseEntityChecker {
	return func(c *gc.C, entity *mongodoc.BaseEntity) {
		c.Assert(entity.ChannelACLs, jc.DeepEquals, acls)
	}
}

func hasAllACLs(user string) baseEntityChecker {
	userACL := mongodoc.ACL{
		Read:  []string{user},
		Write: []string{user},
	}
	return hasACLs(map[params.Channel]mongodoc.ACL{
		params.UnpublishedChannel: userACL,
		params.EdgeChannel:        userACL,
		params.BetaChannel:        userACL,
		params.CandidateChannel:   userACL,
		params.StableChannel:      userACL,
	})
}

func hasChannelEntities(ce map[params.Channel]map[string]*charm.URL) baseEntityChecker {
	return func(c *gc.C, entity *mongodoc.BaseEntity) {
		c.Assert(entity.ChannelEntities, jc.DeepEquals, ce)
	}
}
