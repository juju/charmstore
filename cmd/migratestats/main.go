package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/juju/loggo"
	errgo "gopkg.in/errgo.v1"
	mgo "gopkg.in/mgo.v2"

	"gopkg.in/juju/charmstore.v5/config"
	"gopkg.in/juju/charmstore.v5/internal/charmstore"
)

var (
	logger        = loggo.GetLogger("migratestats")
	loggingConfig = flag.String("logging-config", "INFO", "specify log levels for modules e.g. <root>=TRACE")
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: migratestats [flags] <config path>\n")
		flag.PrintDefaults()
		os.Exit(2)
	}
	flag.Parse()
	if flag.NArg() != 1 {
		flag.Usage()
	}
	if *loggingConfig != "" {
		if err := loggo.ConfigureLoggers(*loggingConfig); err != nil {
			fmt.Fprintf(os.Stderr, "cannot configure loggers: %v", err)
			os.Exit(1)
		}
	}
	if err := run(flag.Arg(0)); err != nil {
		fmt.Fprintf(os.Stderr, "stat counters migration failed: %v\n", err)
		fmt.Fprintf(os.Stderr, "error details: %s\n", errgo.Details(err))
		os.Exit(1)
	}
}

func run(confPath string) error {
	config, err := config.Read(confPath)
	if err != nil {
		return errgo.Notef(err, "cannot read config file %q", confPath)
	}

	logger.Infof("connecting to mongo")
	session, err := mgo.Dial(config.MongoURL)
	if err != nil {
		return errgo.Notef(err, "cannot dial mongo at %q", config.MongoURL)
	}
	// The aggregation query takes a very long time, so disable the timeout.
	session.SetCursorTimeout(0)
	session.SetSocketTimeout(10 * 24 * time.Hour)
	
	// The query might cause MongoDB to be slow, so up the sync timeout a bit.
	session.SetSyncTimeout(time.Minute)

	defer session.Close()
	return run1(session)
}

func run1(session *mgo.Session) error {
	db := session.DB("juju")
	migrations, err := getExecutedMigrations(db)
	if err != nil {
		return errgo.Mask(err)
	}
	if !migrations[charmstore.MigrationStatCounterSquash] {
		logger.Infof("starting stat counter squash")
		if err := squashStats(db); err != nil {
			return errgo.Notef(err, "cannot squash stats")
		}
		if err := setExecutedMigration(db, charmstore.MigrationStatCounterSquash); err != nil {
			return errgo.Mask(err)
		}
	}
	if !migrations[charmstore.MigrationStatCounterReorderKey] {
		logger.Infof("starting key reordering")
		if err := reorderStatsKeys(db); err != nil {
			return errgo.Notef(err, "cannot squash stats")
		}
		if err := setExecutedMigration(db, charmstore.MigrationStatCounterReorderKey); err != nil {
			return errgo.Mask(err)
		}
	}
	logger.Infof("stat counters migration done")
	return nil
}
