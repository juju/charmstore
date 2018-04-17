package v5_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/juju/idmclient"
	"github.com/juju/idmclient/idmtest"
	"github.com/juju/loggo"
	jujutesting "github.com/juju/testing"
	"github.com/juju/testing/httptesting"
	gc "gopkg.in/check.v1"
	"gopkg.in/goose.v2/client"
	"gopkg.in/goose.v2/identity"
	"gopkg.in/goose.v2/swift"
	"gopkg.in/goose.v2/testing/httpsuite"
	"gopkg.in/goose.v2/testservices/openstackservice"
	"gopkg.in/juju/charm.v6"
	"gopkg.in/juju/charmrepo.v3/csclient/params"
	"gopkg.in/macaroon-bakery.v2-unstable/bakery/checkers"
	"gopkg.in/macaroon-bakery.v2-unstable/bakerytest"
	"gopkg.in/macaroon-bakery.v2-unstable/httpbakery"
	"gopkg.in/macaroon.v2-unstable"
	"gopkg.in/mgo.v2"

	"gopkg.in/juju/charmstore.v5/internal/blobstore"
	"gopkg.in/juju/charmstore.v5/internal/charmstore"
	"gopkg.in/juju/charmstore.v5/internal/router"
	"gopkg.in/juju/charmstore.v5/internal/storetesting"
	"gopkg.in/juju/charmstore.v5/internal/v5"
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

	swift *swift.Client
	httpsuite.HTTPSuite
	openstack     *openstackservice.Openstack
	openstackCred *identity.Credentials
}

func (s *commonSuite) SetUpSuite(c *gc.C) {
	s.IsolatedMgoSuite.SetUpSuite(c)
	s.HTTPSuite.SetUpSuite(c)
	if s.enableES {
		s.esSuite = new(storetesting.ElasticSearchSuite)
		s.esSuite.SetUpSuite(c)
	}
}

func (s *commonSuite) TearDownSuite(c *gc.C) {
	s.HTTPSuite.TearDownSuite(c)
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
	s.openstack.Stop()
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
	if s.termsDischarger != nil {
		s.termsDischarger.Close()
	}
	s.IsolatedMgoSuite.TearDownTest(c)
}

// startServer creates a new charmstore server.
func (s *commonSuite) startServer(c *gc.C) {
	// Disable group caching.
	s.PatchValue(&v5.PermCacheExpiry, time.Duration(0))
	// Set up an Openstack service.
	s.openstackCred = &identity.Credentials{
		URL:        s.Server.URL,
		User:       "fred",
		Secrets:    "secret",
		Region:     "heaven",
		TenantName: "awesomo",
	}
	var logMsg []string
	s.openstack, logMsg = openstackservice.New(s.openstackCred, identity.AuthUserPass, false)
	for _, msg := range logMsg {
		c.Logf(msg)
	}
	client := client.NewClient(s.openstackCred, identity.AuthUserPass, nil)
	s.swift = swift.New(client)
	s.openstack.SetupHTTP(nil)
	s.swift.CreateContainer("testc", swift.Private)

	config := charmstore.ServerParams{
		AuthUsername:      testUsername,
		AuthPassword:      testPassword,
		StatsCacheMaxAge:  time.Nanosecond,
		MaxMgoSessions:    s.maxMgoSessions,
		MinUploadPartSize: 10,
		NewBlobBackend:    s.newBlobBackend,
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
	if s.enableTerms {
		s.dischargeTerms = noDischarge
		termsDischarger := bakerytest.NewDischarger(nil, func(_ *http.Request, cond string, arg string) ([]checkers.Caveat, error) {
			return s.dischargeTerms(cond, arg)
		})
		config.TermsLocation = termsDischarger.Location()
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
	c.Assert(err, gc.Equals, nil)
	s.srvParams = config

	if s.enableIdentity {
		config.IdentityLocation = ""
		config.PublicKeyLocator = nil
		s.noMacaroonSrv, err = charmstore.NewServer(db, si, config, map[string]charmstore.NewAPIHandlerFunc{"v5": v5.NewAPIHandler})
		c.Assert(err, gc.Equals, nil)
	} else {
		s.noMacaroonSrv = s.srv
	}
	s.noMacaroonSrvParams = config
	s.store = s.srv.Pool().Store()
}

func (s *commonSuite) newBlobBackend(db *mgo.Database) blobstore.Backend {
	return blobstore.NewSwiftBackend(s.openstackCred, identity.AuthUserPass, "testc")
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
	c.Assert(err, gc.Equals, nil)

	var resources map[string]int
	if len(ch.Meta().Resources) > 0 {
		resources = make(map[string]int)
		// The charm has resources. Ensure that all the required resources are uploaded,
		// then publish with them.
		resDocs, err := s.store.ListResources(id, params.UnpublishedChannel)
		c.Assert(err, gc.Equals, nil)
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
	c.Assert(err, gc.Equals, nil)
	err = s.store.Publish(rurl, resources, params.StableChannel)
	c.Assert(err, gc.Equals, nil)
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
	c.Assert(err, gc.Equals, nil)
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
		c.Assert(err, gc.Equals, nil)
	}
}

// handler returns a request handler that can be
// used to invoke private methods. The caller
// is responsible for calling Put on the returned handler.
func (s *commonSuite) handler(c *gc.C) *v5.ReqHandler {
	h, err := v5.New(s.store.Pool(), s.srvParams, "")
	c.Assert(err, gc.Equals, nil)
	defer h.Close()
	rh, err := h.NewReqHandler(new(http.Request))
	c.Assert(err, gc.Equals, nil)
	// It would be nice if we could call s.AddCleanup here
	// to call rh.Put when the test has completed, but
	// unfortunately CleanupSuite.TearDownTest runs
	// after MgoSuite.TearDownTest, so that's not an option.
	return rh
}

func storeURL(path string) string {
	return "/v5/" + path
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
	c.Assert(err, gc.Equals, nil)
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
	c.Assert(err, gc.Equals, nil)
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

// uploadResource uploads content to the resource with the given name associated with the
// charm with the given id.
func (s *commonSuite) uploadResource(c *gc.C, id *router.ResolvedURL, name string, content string) {
	hash := hashOfString(content)
	_, err := s.store.UploadResource(id, name, strings.NewReader(content), hash, int64(len(content)))
	c.Assert(err, gc.Equals, nil)
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
