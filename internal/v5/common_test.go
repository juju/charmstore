package v5_test // import "gopkg.in/juju/charmstore.v5-unstable/internal/v5"

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"time"

	"github.com/juju/loggo"
	jujutesting "github.com/juju/testing"
	"github.com/juju/testing/httptesting"
	"github.com/julienschmidt/httprouter"
	gc "gopkg.in/check.v1"
	"gopkg.in/juju/charm.v6-unstable"
	"gopkg.in/juju/charmrepo.v2-unstable/csclient/params"
	"gopkg.in/macaroon-bakery.v1/bakery"
	"gopkg.in/macaroon-bakery.v1/bakery/checkers"
	"gopkg.in/macaroon-bakery.v1/bakerytest"
	"gopkg.in/macaroon-bakery.v1/httpbakery"
	"gopkg.in/macaroon.v1"
	"gopkg.in/mgo.v2"

	"gopkg.in/juju/charmstore.v5-unstable/internal/charmstore"
	"gopkg.in/juju/charmstore.v5-unstable/internal/router"
	"gopkg.in/juju/charmstore.v5-unstable/internal/storetesting"
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

	// discharge holds the function that will be used
	// to check third party caveats by the mock
	// discharger. This will be ignored if enableIdentity was
	// not true before commonSuite.SetUpTest is invoked.
	//
	// It may be set by tests to influence the behavior of the
	// discharger.
	discharge func(cav, arg string) ([]checkers.Caveat, error)

	discharger *bakerytest.Discharger
	idM        *idM
	idMServer  *httptest.Server

	dischargeTerms  func(cav, arg string) ([]checkers.Caveat, error)
	termsDischarger *bakerytest.Discharger
	enableTerms     bool

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
		s.idM = newIdM()
		s.idMServer = httptest.NewServer(s.idM)
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
	if s.discharger != nil {
		s.discharger.Close()
		s.idMServer.Close()
	}
	if s.termsDischarger != nil {
		s.termsDischarger.Close()
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
	keyring := bakery.NewPublicKeyRing()
	if s.enableIdentity {
		s.discharge = noDischarge
		discharger := bakerytest.NewDischarger(nil, func(_ *http.Request, cond string, arg string) ([]checkers.Caveat, error) {
			return s.discharge(cond, arg)
		})
		config.IdentityLocation = discharger.Location()
		config.IdentityAPIURL = s.idMServer.URL
		pk, err := httpbakery.PublicKeyForLocation(http.DefaultClient, discharger.Location())
		c.Assert(err, gc.IsNil)
		err = keyring.AddPublicKeyForLocation(discharger.Location(), true, pk)
		c.Assert(err, gc.IsNil)
		c.Logf("added public key for location %v", discharger.Location())
	}
	if s.enableTerms {
		s.dischargeTerms = noDischarge
		termsDischarger := bakerytest.NewDischarger(nil, func(_ *http.Request, cond string, arg string) ([]checkers.Caveat, error) {
			return s.dischargeTerms(cond, arg)
		})
		config.TermsLocation = termsDischarger.Location()
		pk, err := httpbakery.PublicKeyForLocation(http.DefaultClient, termsDischarger.Location())
		c.Assert(err, gc.IsNil)
		err = keyring.AddPublicKeyForLocation(termsDischarger.Location(), true, pk)
		c.Assert(err, gc.IsNil)
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
	s.srv, err = charmstore.NewServer(db, si, config, map[string]charmstore.NewAPIHandlerFunc{"v5": v5.NewAPIHandler})
	c.Assert(err, gc.IsNil)
	s.srvParams = config

	if s.enableIdentity {
		config.IdentityLocation = ""
		config.PublicKeyLocator = nil
		config.IdentityAPIURL = ""
		s.noMacaroonSrv, err = charmstore.NewServer(db, si, config, map[string]charmstore.NewAPIHandlerFunc{"v5": v5.NewAPIHandler})
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

// addPublicCharm adds the given charm to the store and makes it public
// by publishing it to the stable channel.
// It also adds any required resources that haven't already been uploaded
// with the content "<resourcename> content".
func (s *commonSuite) addPublicCharm(c *gc.C, ch charm.Charm, id *router.ResolvedURL) (*router.ResolvedURL, charm.Charm) {
	err := s.store.AddCharmWithArchive(id, ch)
	c.Assert(err, gc.IsNil)

	var resources map[string]int
	if len(ch.Meta().Resources) > 0 {
		resources = make(map[string]int)
		// The charm has resources. Ensure that all the required resources are uploaded,
		// then publish with them.
		resDocs, err := s.store.ListResources(id, params.UnpublishedChannel)
		c.Assert(err, gc.IsNil)
		for _, doc := range resDocs {
			if doc.Revision == -1 {
				// The resource doesn't exist so upload one.
				s.uploadResource(c, id, doc.Name, doc.Name+" content")
				doc.Revision = 0
			}
			resources[doc.Name] = doc.Revision
		}
	}
	s.setPublicWithResources(c, id, resources)
	return id, ch
}

func (s *commonSuite) setPublicWithResources(c *gc.C, rurl *router.ResolvedURL, resources map[string]int) {
	err := s.store.SetPerms(&rurl.URL, "stable.read", params.Everyone)
	c.Assert(err, gc.IsNil)
	err = s.store.Publish(rurl, resources, params.StableChannel)
	c.Assert(err, gc.IsNil)
}

func (s *commonSuite) setPublic(c *gc.C, rurl *router.ResolvedURL) {
	s.setPublicWithResources(c, rurl, nil)
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
func (s *commonSuite) handler(c *gc.C) *v5.ReqHandler {
	h := v5.New(s.store.Pool(), s.srvParams, "")
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
	return "/v5/" + path
}

func (s *commonSuite) bakeryDoAsUser(c *gc.C, user string) func(*http.Request) (*http.Response, error) {
	bclient := httpbakery.NewClient()
	m, err := s.store.Bakery.NewMacaroon("", nil, []checkers.Caveat{
		checkers.DeclaredCaveat("username", user),
	})
	c.Assert(err, gc.IsNil)
	macaroonCookie, err := httpbakery.NewCookie(macaroon.Slice{m})
	c.Assert(err, gc.IsNil)
	return func(req *http.Request) (*http.Response, error) {
		req.AddCookie(macaroonCookie)
		if req.Body == nil {
			return bclient.Do(req)
		}
		body := req.Body.(io.ReadSeeker)
		req.Body = nil
		return bclient.DoWithBody(req, body)
	}
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
	old := s.discharge
	if user != "" {
		s.discharge = dischargeForUser(user)
	} else {
		s.discharge = noDischarge
	}
	defer func() {
		s.discharge = old
	}()
	f()
}

// uploadResource uploads content to the resource with the given name associated with the
// charm with the given id.
func (s *commonSuite) uploadResource(c *gc.C, id *router.ResolvedURL, name string, content string) {
	hash := hashOfString(content)
	_, err := s.store.UploadResource(id, name, strings.NewReader(content), hash, int64(len(content)))
	c.Assert(err, gc.IsNil)
}

func bakeryDo(client *http.Client) func(*http.Request) (*http.Response, error) {
	if client == nil {
		client = httpbakery.NewHTTPClient()
	}
	bclient := httpbakery.NewClient()
	bclient.Client = client
	return func(req *http.Request) (*http.Response, error) {
		if req.Body == nil {
			return bclient.Do(req)
		}
		body := req.Body.(io.ReadSeeker)
		req.Body = nil
		return bclient.DoWithBody(req, body)
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
