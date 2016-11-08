package v4_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/juju/idmclient"
	"github.com/juju/idmclient/idmtest"
	"github.com/juju/loggo"
	jujutesting "github.com/juju/testing"
	"github.com/juju/testing/httptesting"
	"github.com/julienschmidt/httprouter"
	gc "gopkg.in/check.v1"
	"gopkg.in/juju/charm.v6-unstable"
	"gopkg.in/juju/charmrepo.v2-unstable/csclient/params"
	"gopkg.in/macaroon-bakery.v2-unstable/bakery"
	"gopkg.in/macaroon-bakery.v2-unstable/bakery/checkers"
	"gopkg.in/macaroon-bakery.v2-unstable/httpbakery"
	macaroon "gopkg.in/macaroon.v2-unstable"
	"gopkg.in/mgo.v2"

	"gopkg.in/juju/charmstore.v5-unstable/internal/charmstore"
	"gopkg.in/juju/charmstore.v5-unstable/internal/router"
	"gopkg.in/juju/charmstore.v5-unstable/internal/storetesting"
	"gopkg.in/juju/charmstore.v5-unstable/internal/v4"
	"gopkg.in/juju/charmstore.v5-unstable/internal/v5"
)

var mgoLogger = loggo.GetLogger("mgo")

func init() {
	mgo.SetLogger(mgoLog{})
}

type mgoLog struct{}

func (mgoLog) Output(calldepth int, s string) error {
	mgoLogger.LogCallf(calldepth+1, loggo.INFO, "%s", s)
	return nil
}

type commonSuite struct {
	jujutesting.IsolatedMgoSuite

	// srv holds the store HTTP handler.
	srv *charmstore.Server

	// srvParams holds the parameters that the
	// srv handler was started with
	srvParams charmstore.ServerParams

	// noMacaroonSrv holds the store HTTP handler
	// for an instance of the store without identity
	// enabled. If enableIdentity is false, this is
	// the same as srv.
	noMacaroonSrv *charmstore.Server

	// noMacaroonSrvParams holds the parameters that the
	// noMacaroonSrv handler was started with
	noMacaroonSrvParams charmstore.ServerParams

	// store holds an instance of *charm.Store
	// that can be used to access the charmstore database
	// directly.
	store *charmstore.Store

	// esSuite is set only when enableES is set to true.
	esSuite *storetesting.ElasticSearchSuite

	// idmServer holds the fake IDM server instance. It
	// is only non-nil when enableIdentity is true.
	idmServer *idmtest.Server

	// The following fields may be set before
	// SetUpSuite is invoked on commonSuite
	// and influences how the suite sets itself up.

	// enableIdentity holds whether the charmstore server
	// will be started with a configured identity service.
	enableIdentity bool

	// enableES holds whether the charmstore server will be
	// started with Elastic Search enabled.
	enableES bool

	// maxMgoSessions specifies the value that will be given
	// to config.MaxMgoSessions when calling charmstore.NewServer.
	maxMgoSessions int
}

func (s *commonSuite) SetUpSuite(c *gc.C) {
	s.IsolatedMgoSuite.SetUpSuite(c)
	if s.enableES {
		s.esSuite = new(storetesting.ElasticSearchSuite)
		s.esSuite.SetUpSuite(c)
	}
}

func (s *commonSuite) TearDownSuite(c *gc.C) {
	if s.esSuite != nil {
		s.esSuite.TearDownSuite(c)
	}
}

func (s *commonSuite) SetUpTest(c *gc.C) {
	s.IsolatedMgoSuite.SetUpTest(c)
	if s.esSuite != nil {
		s.esSuite.SetUpTest(c)
	}
	if s.enableIdentity {
		s.idmServer = idmtest.NewServer()
	}
	s.startServer(c)
}

func (s *commonSuite) TearDownTest(c *gc.C) {
	s.store.Pool().Close()
	s.store.Close()
	s.srv.Close()
	s.noMacaroonSrv.Close()
	if s.esSuite != nil {
		s.esSuite.TearDownTest(c)
	}
	if s.idmServer != nil {
		s.idmServer.Close()
	}
	s.IsolatedMgoSuite.TearDownTest(c)
}

// startServer creates a new charmstore server.
func (s *commonSuite) startServer(c *gc.C) {
	// Disable group caching.
	s.PatchValue(&v5.PermCacheExpiry, time.Duration(0))
	config := charmstore.ServerParams{
		AuthUsername:     testUsername,
		AuthPassword:     testPassword,
		StatsCacheMaxAge: time.Nanosecond,
		MaxMgoSessions:   s.maxMgoSessions,
		AgentUsername:    "notused",
		AgentKey:         new(bakery.KeyPair),
	}
	keyring := httpbakery.NewPublicKeyRing(nil, nil)
	keyring.AllowInsecure()
	if s.enableIdentity {
		s.idmServer = idmtest.NewServer()
		config.AgentUsername = "charmstore-agent"
		s.idmServer.AddUser(config.AgentUsername)
		config.AgentKey = s.idmServer.UserPublicKey(config.AgentUsername)
		config.IdentityLocation = s.idmServer.URL.String()
		c.Logf("added public key for location %v", config.IdentityLocation)
	}
	config.PublicKeyLocator = keyring
	var si *charmstore.SearchIndex
	if s.enableES {
		si = &charmstore.SearchIndex{
			Database: s.esSuite.ES,
			Index:    s.esSuite.TestIndex,
		}
	}
	db := s.Session.DB("charmstore")
	var err error
	s.srv, err = charmstore.NewServer(db, si, config, map[string]charmstore.NewAPIHandlerFunc{"v4": v4.NewAPIHandler})
	c.Assert(err, gc.IsNil)
	s.srvParams = config

	if s.enableIdentity {
		config.IdentityLocation = ""
		config.PublicKeyLocator = nil
		config.IdentityAPIURL = ""
		s.noMacaroonSrv, err = charmstore.NewServer(db, si, config, map[string]charmstore.NewAPIHandlerFunc{"v4": v4.NewAPIHandler})
		c.Assert(err, gc.IsNil)
	} else {
		s.noMacaroonSrv = s.srv
	}
	s.noMacaroonSrvParams = config
	s.store = s.srv.Pool().Store()
}

func (s *commonSuite) addPublicCharmFromRepo(c *gc.C, charmName string, rurl *router.ResolvedURL) (*router.ResolvedURL, charm.Charm) {
	return s.addPublicCharm(c, storetesting.Charms.CharmDir(charmName), rurl)
}

func (s *commonSuite) addPublicCharm(c *gc.C, ch charm.Charm, rurl *router.ResolvedURL) (*router.ResolvedURL, charm.Charm) {
	err := s.store.AddCharmWithArchive(rurl, ch)
	c.Assert(err, gc.IsNil)
	s.setPublic(c, rurl)
	return rurl, ch
}

func (s *commonSuite) setPublic(c *gc.C, rurl *router.ResolvedURL) {
	err := s.store.SetPerms(&rurl.URL, "stable.read", params.Everyone)
	c.Assert(err, gc.IsNil)
	err = s.store.Publish(rurl, nil, params.StableChannel)
	c.Assert(err, gc.IsNil)
}

func (s *commonSuite) addPublicBundleFromRepo(c *gc.C, bundleName string, rurl *router.ResolvedURL, addRequiredCharms bool) (*router.ResolvedURL, charm.Bundle) {
	return s.addPublicBundle(c, storetesting.Charms.BundleDir(bundleName), rurl, addRequiredCharms)
}

func (s *commonSuite) addPublicBundle(c *gc.C, bundle charm.Bundle, rurl *router.ResolvedURL, addRequiredCharms bool) (*router.ResolvedURL, charm.Bundle) {
	if addRequiredCharms {
		s.addRequiredCharms(c, bundle)
	}
	err := s.store.AddBundleWithArchive(rurl, bundle)
	c.Assert(err, gc.IsNil)
	s.setPublic(c, rurl)
	return rurl, bundle
}

// addCharms adds all the given charms to s.store. The
// map key is the id of the charm.
func (s *commonSuite) addCharms(c *gc.C, charms map[string]charm.Charm) {
	for id, ch := range charms {
		s.addPublicCharm(c, storetesting.NewCharm(ch.Meta()), mustParseResolvedURL(id))
	}
}

// setPerms sets the stable channel read permissions of a set of
// entities. The map key is the is the id of each entity; its associated
// value is its read ACL.
func (s *commonSuite) setPerms(c *gc.C, readACLs map[string][]string) {
	for url, acl := range readACLs {
		err := s.store.SetPerms(charm.MustParseURL(url), "stable.read", acl...)
		c.Assert(err, gc.IsNil)
	}
}

// handler returns a request handler that can be
// used to invoke private methods. The caller
// is responsible for calling Put on the returned handler.
func (s *commonSuite) handler(c *gc.C) v4.ReqHandler {
	h, err := v4.New(s.store.Pool(), s.srvParams, "")
	c.Assert(err, gc.IsNil)
	defer h.Close()
	rh, err := h.NewReqHandler(new(http.Request))
	c.Assert(err, gc.IsNil)
	// It would be nice if we could call s.AddCleanup here
	// to call rh.Put when the test has completed, but
	// unfortunately CleanupSuite.TearDownTest runs
	// after MgoSuite.TearDownTest, so that's not an option.
	return rh
}

func storeURL(path string) string {
	return "/v4/" + path
}

func (s *commonSuite) bakeryDoAsUser(user string) func(*http.Request) (*http.Response, error) {
	return bakeryDo(s.idmServer.Client(user))
}

// login returns a bakery client that holds a macaroon that
// declares the given authenticated user.
// The login macaroon will be attenuated by the given first
// party caveat conditions.
func (s *commonSuite) login(user string, conditions ...string) *httpbakery.Client {
	var caveats []checkers.Caveat
	for _, cond := range conditions {
		caveats = append(caveats, checkers.Caveat{
			Condition: cond,
		})
	}
	caveats = append(caveats, idmclient.UserDeclaration(user))
	m, err := s.store.Bakery.NewMacaroon(caveats)
	if err != nil {
		panic(err)
	}
	client := httpbakery.NewClient()
	u, err := url.Parse("http://127.0.0.1")
	if err != nil {
		panic(err)
	}
	err = httpbakery.SetCookie(client.Jar, u, macaroon.Slice{m})
	if err != nil {
		panic(err)
	}
	return client
}

// addRequiredCharms adds any charms required by the given
// bundle that are not already in the store.
func (s *commonSuite) addRequiredCharms(c *gc.C, bundle charm.Bundle) {
	for _, svc := range bundle.Data().Applications {
		u := charm.MustParseURL(svc.Charm)
		if _, err := s.store.FindBestEntity(u, params.StableChannel, nil); err == nil {
			continue
		}
		if u.Revision == -1 {
			u.Revision = 0
		}
		var rurl router.ResolvedURL
		rurl.URL = *u
		chDir, err := charm.ReadCharmDir(storetesting.Charms.CharmDirPath(u.Name))
		ch := charm.Charm(chDir)
		if err != nil {
			// The charm doesn't exist in the local charm repo; make one up.
			ch = storetesting.NewCharm(nil)
		}
		if len(ch.Meta().Series) == 0 && u.Series == "" {
			rurl.URL.Series = "trusty"
		}
		if u.User == "" {
			rurl.URL.User = "charmers"
			rurl.PromulgatedRevision = rurl.URL.Revision
		} else {
			rurl.PromulgatedRevision = -1
		}
		c.Logf("adding charm %v %d required by bundle to fulfil %v", &rurl.URL, rurl.PromulgatedRevision, svc.Charm)
		s.addPublicCharm(c, ch, &rurl)
	}
}

func (s *commonSuite) assertPut(c *gc.C, url string, val interface{}) {
	s.assertPut0(c, url, val, false)
}

func (s *commonSuite) assertPutAsAdmin(c *gc.C, url string, val interface{}) {
	s.assertPut0(c, url, val, true)
}

func (s *commonSuite) assertPut0(c *gc.C, url string, val interface{}, asAdmin bool) {
	body, err := json.Marshal(val)
	c.Assert(err, gc.IsNil)
	p := httptesting.JSONCallParams{
		Handler: s.srv,
		URL:     storeURL(url),
		Method:  "PUT",
		Do:      bakeryDo(nil),
		Header: http.Header{
			"Content-Type": {"application/json"},
		},
		Body: bytes.NewReader(body),
	}
	if asAdmin {
		p.Username = testUsername
		p.Password = testPassword
	}
	httptesting.AssertJSONCall(c, p)
}

func (s *commonSuite) assertGet(c *gc.C, url string, expectVal interface{}) {
	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler:    s.srv,
		Do:         bakeryDo(nil),
		URL:        storeURL(url),
		ExpectBody: expectVal,
	})
}

// assertGetIsUnauthorized asserts that a GET to the given URL results
// in an ErrUnauthorized response with the given error message.
func (s *commonSuite) assertGetIsUnauthorized(c *gc.C, url, expectMessage string) {
	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler:      s.srv,
		Do:           bakeryDo(nil),
		Method:       "GET",
		URL:          storeURL(url),
		ExpectStatus: http.StatusUnauthorized,
		ExpectBody: params.Error{
			Code:    params.ErrUnauthorized,
			Message: expectMessage,
		},
	})
}

// assertGetIsUnauthorized asserts that a PUT to the given URL with the
// given body value results in an ErrUnauthorized response with the given
// error message.
func (s *commonSuite) assertPutIsUnauthorized(c *gc.C, url string, val interface{}, expectMessage string) {
	body, err := json.Marshal(val)
	c.Assert(err, gc.IsNil)
	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler: s.srv,
		URL:     storeURL(url),
		Method:  "PUT",
		Do:      bakeryDo(nil),
		Header: http.Header{
			"Content-Type": {"application/json"},
		},
		Body:         bytes.NewReader(body),
		ExpectStatus: http.StatusUnauthorized,
		ExpectBody: params.Error{
			Code:    params.ErrUnauthorized,
			Message: expectMessage,
		},
	})
}

// doAsUser calls the given function, discharging any authorization
// request as the given user name.
// If user is empty, no discharge will be allowed.
func (s *commonSuite) doAsUser(user string, f func()) {
	s.idmServer.SetDefaultUser(user)
	defer s.idmServer.SetDefaultUser("")
	f()
}

func bakeryDo(client *httpbakery.Client) func(*http.Request) (*http.Response, error) {
	if client == nil {
		client = httpbakery.NewClient()
	}
	return func(req *http.Request) (*http.Response, error) {
		if req.Body == nil {
			return client.Do(req)
		}
		body := req.Body.(io.ReadSeeker)
		req.Body = nil
		return client.DoWithBody(req, body)
	}
}

type idM struct {
	// groups may be set to determine the mapping
	// from user to groups for that user.
	groups map[string][]string

	// body may be set to cause serveGroups to return
	// an arbitrary HTTP response body.
	body string

	// contentType is the contentType to use when body is not ""
	contentType string

	// status may be set to indicate the HTTP status code
	// when body is not nil.
	status int

	router *httprouter.Router
}

func newIdM() *idM {
	idM := &idM{
		groups: make(map[string][]string),
		router: httprouter.New(),
	}
	idM.router.GET("/v1/u/:user/groups", idM.serveGroups)
	idM.router.GET("/v1/u/:user/idpgroups", idM.serveGroups)
	return idM
}

func (idM *idM) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	idM.router.ServeHTTP(w, req)
}

func (idM *idM) serveGroups(w http.ResponseWriter, req *http.Request, p httprouter.Params) {
	if idM.body != "" {
		if idM.contentType != "" {
			w.Header().Set("Content-Type", idM.contentType)
		}
		if idM.status != 0 {
			w.WriteHeader(idM.status)
		}
		w.Write([]byte(idM.body))
		return
	}
	u := p.ByName("user")
	if u == "" {
		panic("no user")
	}
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	if err := enc.Encode(idM.groups[u]); err != nil {
		panic(err)
	}
}
