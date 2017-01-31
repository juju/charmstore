// Copyright 2016 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package main // import "gopkg.in/juju/charmstore.v5-unstable/cmd/blobtool"

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/juju/loggo"
	"gopkg.in/errgo.v1"
	"gopkg.in/mgo.v2"
	"gopkg.in/natefinch/lumberjack.v2"

	"gopkg.in/juju/charmstore.v5-unstable/config"
	"gopkg.in/juju/charmstore.v5-unstable/internal/charmstore"
	"gopkg.in/juju/charmstore.v5-unstable/internal/mongodoc"
)

var (
	logger        = loggo.GetLogger("blobtool")
	loggingConfig = flag.String("logging-config", "", "specify log levels for modules e.g. TRACE or '<root>=DEBUG;mgo=WARN'")
	filter        = flag.String("filter", "", `json filter string to use as mongodb query. e.g. '{"user":"evarlast"}' or '{"size":{"$lt": 10000000}}'`)
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s [options] <config path>\n", filepath.Base(os.Args[0]))
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
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}

func run(confPath string) error {
	logger.Infof("reading configuration")
	conf, err := config.Read(confPath)
	if err != nil {
		return errgo.Notef(err, "cannot read config file %q", confPath)
	}

	logger.Infof("connecting to mongo")
	session, err := mgo.Dial(conf.MongoURL)
	if err != nil {
		return errgo.Notef(err, "cannot dial mongo at %q", conf.MongoURL)
	}
	defer session.Close()
	dbName := "juju"
	if conf.Database != "" {
		dbName = conf.Database
	}
	db := session.DB(dbName)

	if conf.BlobStorageProviders == nil {
		return errgo.New("a provider-config section must be in the input config file")
	}

	logger.Infof("instantiating the store")
	cfg := charmstore.ServerParams{
		AuthUsername:            conf.AuthUsername,
		AuthPassword:            conf.AuthPassword,
		IdentityLocation:        conf.IdentityLocation,
		IdentityAPIURL:          conf.IdentityAPIURL,
		TermsLocation:           conf.TermsLocation,
		AgentUsername:           conf.AgentUsername,
		AgentKey:                conf.AgentKey,
		StatsCacheMaxAge:        conf.StatsCacheMaxAge.Duration,
		MaxMgoSessions:          conf.MaxMgoSessions,
		HTTPRequestWaitDuration: conf.RequestTimeout.Duration,
		SearchCacheMaxAge:       conf.SearchCacheMaxAge.Duration,
		BlobStorageProviders:    conf.BlobStorageProviders,
	}

	if conf.AuditLogFile != "" {
		cfg.AuditLogger = &lumberjack.Logger{
			Filename: conf.AuditLogFile,
			MaxSize:  conf.AuditLogMaxSize,
			MaxAge:   conf.AuditLogMaxAge,
		}
	}

	pool, err := charmstore.NewPool(db, nil, nil, cfg)
	if err != nil {
		return errgo.Notef(err, "cannot create a new store")
	}
	store := pool.Store()
	defer store.Close()

	logger.Infof("updating entities")
	if err := action(store); err != nil {
		return errgo.Notef(err, "cannot update entities")
	}

	logger.Infof("done")
	return nil
}

func action(store *charmstore.Store) error {
	entities := store.DB.Entities()
	var filterMap map[string]interface{}
	err := json.Unmarshal([]byte(*filter), &filterMap)
	if err != nil {
		return errgo.Notef(err, "could not json unmarshal filter: %s", filter)
	}
	iter := entities.Find(filterMap).Iter()
	defer iter.Close()

	successCounter := 0
	failCounter := 0
	var entity mongodoc.Entity
	for iter.Next(&entity) {
		logger.Debugf("processing %s", entity.URL)
		blob, err := store.OpenBlob(charmstore.EntityResolvedURL(&entity))
		if err != nil {
			logger.Warningf("cannot open archive data for %s: %s", entity.URL, err)
			failCounter++
			continue
		}
		defer blob.Close()
		logger.Debugf("copying %s %s", entity.URL, entity.BlobName)
		store.BlobStore.Put(blob, entity.BlobName, blob.Size, blob.Hash, nil)
		successCounter++
		if successCounter%100 == 0 {
			logger.Infof("%d entities written", successCounter)
		}
	}
	logger.Infof("%d entities written. %d failed to read.", successCounter, failCounter)

	return nil
}

var mgoLogger = loggo.GetLogger("mgo")

func init() {
	mgo.SetLogger(mgoLog{})
}

type mgoLog struct{}

func (mgoLog) Output(calldepth int, s string) error {
	mgoLogger.LogCallf(calldepth+1, loggo.DEBUG, "%s", s)
	return nil
}
