// Copyright 2012, 2013, 2014 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"path/filepath"

	"github.com/gorilla/handlers"
	"github.com/juju/loggo"
	"gopkg.in/errgo.v1"
	"gopkg.in/goose.v2/identity"
	"gopkg.in/macaroon-bakery.v2-unstable/bakery"
	"gopkg.in/macaroon-bakery.v2-unstable/httpbakery"
	"gopkg.in/mgo.v2"
	"gopkg.in/natefinch/lumberjack.v2"

	"gopkg.in/juju/charmstore.v5"
	"gopkg.in/juju/charmstore.v5/config"
	"gopkg.in/juju/charmstore.v5/elasticsearch"
	"gopkg.in/juju/charmstore.v5/internal/blobstore"
)

var (
	logger        = loggo.GetLogger("charmd")
	loggingConfig = flag.String("logging-config", "", "specify log levels for modules e.g. <root>=TRACE")
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
	if err := serve(flag.Arg(0)); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}

func serve(confPath string) error {
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

	var es *elasticsearch.Database
	if conf.ESAddr != "" {
		es = &elasticsearch.Database{
			Addr: conf.ESAddr,
		}
	}

	keyring := bakery.NewPublicKeyRing()
	err = addPublicKey(keyring, conf.IdentityLocation, conf.IdentityPublicKey)
	if err != nil {
		return errgo.Mask(err)
	}
	if conf.TermsLocation != "" {
		err = addPublicKey(keyring, conf.TermsLocation, conf.TermsPublicKey)
		if err != nil {
			return errgo.Mask(err)
		}
	}

	logger.Infof("setting up the API server")
	cfg := charmstore.ServerParams{
		AuthUsername:            conf.AuthUsername,
		AuthPassword:            conf.AuthPassword,
		IdentityLocation:        conf.IdentityLocation,
		TermsLocation:           conf.TermsLocation,
		AgentUsername:           conf.AgentUsername,
		AgentKey:                conf.AgentKey,
		StatsCacheMaxAge:        conf.StatsCacheMaxAge.Duration,
		MaxMgoSessions:          conf.MaxMgoSessions,
		HTTPRequestWaitDuration: conf.RequestTimeout.Duration,
		SearchCacheMaxAge:       conf.SearchCacheMaxAge.Duration,
		PublicKeyLocator:        keyring,
		MinUploadPartSize:       conf.MinUploadPartSize,
		MaxUploadPartSize:       conf.MaxUploadPartSize,
		MaxUploadParts:          conf.MaxUploadParts,
		RunBlobStoreGC:          true,
	}
	switch conf.BlobStore {
	case config.MongoDBBlobStore:
		// This is the default. No need for a custom function.
	case config.SwiftBlobStore:
		cred := &identity.Credentials{
			URL:        conf.SwiftAuthURL,
			User:       conf.SwiftUsername,
			Secrets:    conf.SwiftSecret,
			Region:     conf.SwiftRegion,
			TenantName: conf.SwiftTenant,
		}
		cfg.NewBlobBackend = func(db *mgo.Database) blobstore.Backend {
			return blobstore.NewSwiftBackend(cred, conf.SwiftAuthMode.Mode, conf.SwiftBucket)
		}
	default:
		return errgo.Newf("unknown blob store type")
	}

	if conf.AuditLogFile != "" {
		cfg.AuditLogger = &lumberjack.Logger{
			Filename: conf.AuditLogFile,
			MaxSize:  conf.AuditLogMaxSize,
			MaxAge:   conf.AuditLogMaxAge,
		}
	}

	server, err := charmstore.NewServer(db, es, "cs", cfg, charmstore.Legacy, charmstore.V4, charmstore.V5)
	if err != nil {
		return errgo.Notef(err, "cannot create new server at %q", conf.APIAddr)
	}
	handler := server.(http.Handler)
	if conf.AccessLog != "" {
		accesslog := &lumberjack.Logger{
			Filename:   conf.AccessLog,
			MaxSize:    500, // megabytes
			MaxBackups: 3,
			MaxAge:     28, //days
		}
		handler = handlers.CombinedLoggingHandler(accesslog, handler)
	}
	logger.Infof("starting the API server")
	return http.ListenAndServe(conf.APIAddr, handler)
}

func addPublicKey(ring *bakery.PublicKeyRing, loc string, key *bakery.PublicKey) error {
	if key != nil {
		return ring.AddPublicKeyForLocation(loc, false, key)
	}
	pubKey, err := httpbakery.PublicKeyForLocation(http.DefaultClient, loc)
	if err != nil {
		return errgo.Mask(err)
	}
	return ring.AddPublicKeyForLocation(loc, false, pubKey)
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
