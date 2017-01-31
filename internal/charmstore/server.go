// Copyright 2014-2016 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

// This is the internal version of the charmstore package.
// It exposes details to the various API packages
// that we do not wish to expose to the world at large.
package charmstore // import "gopkg.in/juju/charmstore.v5-unstable/internal/charmstore"

import (
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/errgo.v1"
	"gopkg.in/macaroon-bakery.v2-unstable/bakery"
	"gopkg.in/macaroon-bakery.v2-unstable/bakery/mgostorage"
	"gopkg.in/mgo.v2"
	"gopkg.in/natefinch/lumberjack.v2"

	"gopkg.in/juju/charmstore.v5-unstable/internal/blobstore"
	"gopkg.in/juju/charmstore.v5-unstable/internal/monitoring"
	"gopkg.in/juju/charmstore.v5-unstable/internal/router"
)

// NewAPIHandlerFunc is a function that returns a new API handler that uses
// the given Store. The absPath parameter holds the root path of the
// API handler.
type NewAPIHandlerFunc func(pool *Pool, p ServerParams, absPath string) (HTTPCloseHandler, error)

// HTTPCloseHandler represents a HTTP handler that
// must be closed after use.
type HTTPCloseHandler interface {
	Close()
	http.Handler
}

// ServerParams holds configuration for a new internal API server.
type ServerParams struct {
	// AuthUsername and AuthPassword hold the credentials
	// used for HTTP basic authentication.
	AuthUsername string
	AuthPassword string

	// IdentityLocation holds the location of the third party authorization
	// service to use when creating third party caveats,
	// for example: http://api.jujucharms.com/identity
	// If it is empty, IdentityAPIURL will be used.
	IdentityLocation string

	// TermsLocations holds the location of the
	// terms service, which knows about user agreements to
	// Terms and Conditions required by the charm.
	TermsLocation string

	// PublicKeyLocator holds a public key store.
	// It may be nil.
	PublicKeyLocator bakery.PublicKeyLocator

	// IdentityAPIURL holds the URL of the identity manager,
	// for example http://api.jujucharms.com/identity
	IdentityAPIURL string

	// AgentUsername and AgentKey hold the credentials used for agent
	// authentication.
	AgentUsername string
	AgentKey      *bakery.KeyPair

	// StatsCacheMaxAge is the maximum length of time between
	// refreshes of entities in the stats cache.
	StatsCacheMaxAge time.Duration

	// SearchCacheMaxAge is the maximum length of time between
	// refreshes of entities in the search cache.
	SearchCacheMaxAge time.Duration

	// MaxMgoSessions specifies a soft limit on the maximum
	// number of mongo sessions used. Each concurrent
	// HTTP request will use one session.
	MaxMgoSessions int

	// HTTPRequestWaitDuration holds the amount of time
	// that an HTTP request will wait for a free connection
	// when the MaxConcurrentHTTPRequests limit is reached.
	HTTPRequestWaitDuration time.Duration

	// AuditLogger optionally holds the logger which will be used to
	// write audit log entries.
	AuditLogger *lumberjack.Logger

	// RootKeyPolicy holds the default policy used when creating
	// macaroon root keys.
	RootKeyPolicy mgostorage.Policy

	// StorageProviders holds a list of storage providers in priority order.
	// During a migration it will likely be configured to s3 and gridfs.
	// AWS recommends using ~/.aws/configuration file for secure credentials,
	// thus this provider does not take credential configuration.
	BlobStorageProviders []blobstore.ProviderConfig
}

const defaultRootKeyExpiryDuration = 24 * time.Hour

// NewServer returns a handler that serves the given charm store API
// versions using db to store that charm store data.
// An optional elasticsearch configuration can be specified in si. If
// elasticsearch is not being used then si can be set to nil.
// The key of the versions map is the version name.
// The handler configuration is provided to all version handlers.
//
// The returned Server should be closed after use.
func NewServer(db *mgo.Database, si *SearchIndex, config ServerParams, versions map[string]NewAPIHandlerFunc) (*Server, error) {
	if len(versions) == 0 {
		return nil, errgo.Newf("charm store server must serve at least one version of the API")
	}
	config.IdentityLocation = strings.TrimSuffix(config.IdentityLocation, "/")
	config.TermsLocation = strings.TrimSuffix(config.TermsLocation, "/")
	config.IdentityAPIURL = strings.TrimSuffix(config.IdentityAPIURL, "/")
	if config.IdentityLocation == "" && config.IdentityAPIURL != "" {
		config.IdentityLocation = config.IdentityAPIURL
	} else if config.IdentityLocation != "" && config.IdentityAPIURL == "" {
		config.IdentityAPIURL = config.IdentityLocation
	}
	logger.Infof("identity discharge location: %s", config.IdentityLocation)
	logger.Infof("identity API location: %s", config.IdentityAPIURL)
	logger.Infof("terms discharge location: %s", config.TermsLocation)
	bparams := bakery.NewServiceParams{
		// TODO The location is attached to any macaroons that we
		// mint. Currently we don't know the location of the current
		// service. We potentially provide a way to configure this,
		// but it probably doesn't matter, as nothing currently uses
		// the macaroon location for anything.
		Location: "charmstore",
		Locator:  config.PublicKeyLocator,
	}
	if config.RootKeyPolicy.ExpiryDuration == 0 {
		config.RootKeyPolicy.ExpiryDuration = defaultRootKeyExpiryDuration
	}
	pool, err := NewPool(db, si, &bparams, config)
	if err != nil {
		return nil, errgo.Notef(err, "cannot make store")
	}
	store := pool.Store()
	defer store.Close()
	if err := migrate(store.DB); err != nil {
		pool.Close()
		return nil, errgo.Notef(err, "database migration failed")
	}
	store.Go(func(store *Store) {
		if err := store.syncSearch(); err != nil {
			logger.Errorf("Cannot populate elasticsearch: %v", err)
		}
	})
	srv := &Server{
		pool: pool,
		mux:  router.NewServeMux(),
	}
	// Version independent API.
	handle(srv.mux, "/debug", newServiceDebugHandler(pool, config, srv.mux))
	handle(srv.mux, "/metrics", prometheusHandler())
	for vers, newAPI := range versions {
		root := "/" + vers
		h, err := newAPI(pool, config, root)
		if err != nil {
			return nil, errgo.Notef(err, "cannot initialize handler for version %v", vers)
		}
		handle(srv.mux, root, h)
		srv.handlers = append(srv.handlers, h)
	}

	return srv, nil
}

func prometheusHandler() http.Handler {
	h := prometheus.Handler()
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		// Use prometheus to monitor its own requests...
		monReq := monitoring.NewRequest(req.Method, "prometheus")
		defer monReq.Done()
		monReq.SetKind("metrics")
		h.ServeHTTP(w, req)
	})
}

type Server struct {
	pool     *Pool
	mux      *router.ServeMux
	handlers []HTTPCloseHandler
}

// ServeHTTP implements http.Handler.ServeHTTP.
func (s *Server) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	s.mux.ServeHTTP(w, req)
}

// Close closes the server. It must be called when the server
// is finished with.
func (s *Server) Close() {
	s.pool.Close()
	for _, h := range s.handlers {
		h.Close()
	}
	s.handlers = nil
}

// Pool returns the Pool used by the server.
func (s *Server) Pool() *Pool {
	return s.pool
}

func handle(mux *router.ServeMux, path string, handler http.Handler) {
	if path != "/" {
		handler = http.StripPrefix(path, handler)
		path += "/"
	}
	mux.Handle(path, handler)
}
