// Copyright 2014 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package v5_test

import (
	"archive/zip"
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/juju/charmrepo/v6/csclient/params"
	charmtesting "github.com/juju/charmrepo/v6/testing"
	"github.com/juju/testing/httptesting"
	gc "gopkg.in/check.v1"
	"gopkg.in/errgo.v1"
	"gopkg.in/macaroon-bakery.v2-unstable/bakery/checkers"
	"gopkg.in/macaroon-bakery.v2-unstable/httpbakery"
	"gopkg.in/macaroon.v2-unstable"
	"gopkg.in/mgo.v2/bson"

	"gopkg.in/juju/charmstore.v5/internal/blobstore"
	"gopkg.in/juju/charmstore.v5/internal/charm"
	"gopkg.in/juju/charmstore.v5/internal/charmstore"
	"gopkg.in/juju/charmstore.v5/internal/mongodoc"
	"gopkg.in/juju/charmstore.v5/internal/router"
	"gopkg.in/juju/charmstore.v5/internal/storetesting"
	"gopkg.in/juju/charmstore.v5/internal/storetesting/stats"
	v5 "gopkg.in/juju/charmstore.v5/internal/v5"
)

type commonArchiveSuite struct {
	commonSuite
}

type ArchiveSuite struct {
	commonArchiveSuite
}

var _ = gc.Suite(&ArchiveSuite{})

func (s *ArchiveSuite) SetUpSuite(c *gc.C) {
	s.enableIdentity = true
	s.commonArchiveSuite.SetUpSuite(c)
	c.Logf("after SetUpSuite, enableIdentity %v", s.enableIdentity)
}

func (s *ArchiveSuite) TestGetCharmWithTermsWhenTermsServiceNotConfigured(c *gc.C) {
	id := newResolvedURL("cs:~charmers/precise/terms-0", -1)
	s.addPublicCharm(c, storetesting.NewCharm(&charm.Meta{
		Terms: []string{"terms-1/1", "terms-2/5"},
	}), id)

	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler:      s.srv,
		URL:          storeURL("~charmers/precise/terms-0/archive"),
		ExpectStatus: http.StatusUnauthorized,
		ExpectBody: params.Error{
			Message: "charmstore not configured to serve charms with terms and conditions",
			Code:    params.ErrUnauthorized,
		},
	})
}

func (s *ArchiveSuite) TestGet(c *gc.C) {
	id := newResolvedURL("cs:~charmers/precise/wordpress-0", -1)
	ch := storetesting.NewCharm(nil)
	s.addPublicCharm(c, ch, id)

	rec := s.assertArchiveDownload(
		c,
		"~charmers/precise/wordpress-0",
		nil,
		ch.Bytes(),
	)
	c.Assert(rec.Header().Get(params.EntityIdHeader), gc.Equals, "cs:~charmers/precise/wordpress-0")
	c.Assert(rec.Header().Get("Content-Disposition"), gc.Equals, "attachment; filename=wordpress.zip")
	assertCacheControl(c, rec.Header(), true)

	// Check that the HTTP range logic is plugged in OK. If this
	// is working, we assume that the whole thing is working OK,
	// as net/http is well-tested.
	rec = httptesting.DoRequest(c, httptesting.DoRequestParams{
		Handler: s.srv,
		URL:     storeURL("~charmers/precise/wordpress-0/archive"),
		Header:  http.Header{"Range": {"bytes=10-100"}},
	})
	c.Assert(rec.Code, gc.Equals, http.StatusPartialContent, gc.Commentf("body: %q", rec.Body.Bytes()))
	c.Assert(rec.Body.Bytes(), gc.HasLen, 100-10+1)
	c.Assert(rec.Body.Bytes(), gc.DeepEquals, ch.Bytes()[10:101])
	c.Assert(rec.Header().Get(params.ContentHashHeader), gc.Equals, hashOfBytes(ch.Bytes()))
	c.Assert(rec.Header().Get(params.EntityIdHeader), gc.Equals, "cs:~charmers/precise/wordpress-0")
	assertCacheControl(c, rec.Header(), true)
}

func (s *ArchiveSuite) TestGetWithPartialId(c *gc.C) {
	id := newResolvedURL("cs:~charmers/precise/wordpress-0", -1)
	ch := storetesting.NewCharm(nil)
	s.addPublicCharm(c, ch, id)

	rec := s.assertArchiveDownload(
		c,
		"~charmers/wordpress",
		nil,
		ch.Bytes(),
	)
	// The complete entity id can be retrieved from the response header.
	c.Assert(rec.Header().Get(params.EntityIdHeader), gc.Equals, id.URL.String())
}

func (s *ArchiveSuite) TestGetPromulgatedWithPartialId(c *gc.C) {
	id := newResolvedURL("cs:~charmers/utopic/wordpress-42", 42)
	ch := storetesting.NewCharm(nil)
	s.addPublicCharm(c, ch, id)

	rec := s.assertArchiveDownload(
		c,
		"wordpress",
		nil,
		ch.Bytes(),
	)
	c.Assert(rec.Header().Get(params.EntityIdHeader), gc.Equals, id.PromulgatedURL().String())
}

func (s *ArchiveSuite) TestGetCounters(c *gc.C) {
	for i, id := range []*router.ResolvedURL{
		newResolvedURL("~who/utopic/mysql-42", 42),
	} {
		c.Logf("test %d: %s", i, id)

		ch := storetesting.NewCharm(nil)
		s.addPublicCharm(c, ch, id)

		// Download the charm archive using the API, which should increment
		// the download counts.
		s.assertArchiveDownload(
			c,
			id.URL.Path(),
			nil,
			ch.Bytes(),
		)

		// Check that the downloads count for the entity has been updated.
		stats.CheckTotalDownloads(c, s.store, &id.URL, 1)
	}
}

func (s *ArchiveSuite) TestGetCountersDisabled(c *gc.C) {
	id := newResolvedURL("~charmers/utopic/mysql-42", 42)
	ch := storetesting.NewCharm(nil)
	s.addPublicCharm(c, ch, id)

	// Download the charm archive using the API, passing stats=0.
	s.assertArchiveDownload(
		c,
		"",
		&httptesting.DoRequestParams{URL: storeURL("~charmers/utopic/mysql-42/archive?stats=0")},
		ch.Bytes(),
	)

	// Check that the downloads count for the entity has not been updated.
	counts, _, err := s.store.ArchiveDownloadCounts(&id.URL)
	c.Assert(err, gc.Equals, nil)
	c.Assert(counts.Total, gc.Equals, int64(0))
}

var archivePostErrorsTests = []struct {
	about           string
	url             string
	noContentLength bool
	noHash          bool
	entity          charmstore.ArchiverTo
	expectStatus    int
	expectMessage   string
	expectCode      params.ErrorCode
}{{
	about:         "revision specified",
	url:           "~charmers/precise/wordpress-23",
	expectStatus:  http.StatusBadRequest,
	expectMessage: "revision specified, but should not be specified",
	expectCode:    params.ErrBadRequest,
}, {
	about:         "no hash given",
	url:           "~charmers/precise/wordpress",
	noHash:        true,
	expectStatus:  http.StatusBadRequest,
	expectMessage: "hash parameter not specified",
	expectCode:    params.ErrBadRequest,
}, {
	about:           "no content length",
	url:             "~charmers/precise/wordpress",
	noContentLength: true,
	expectStatus:    http.StatusBadRequest,
	expectMessage:   "Content-Length not specified",
	expectCode:      params.ErrBadRequest,
}, {
	about:         "invalid channel",
	url:           "~charmers/bad-wolf/trusty/wordpress",
	expectStatus:  http.StatusNotFound,
	expectMessage: "not found",
	expectCode:    params.ErrNotFound,
}, {
	about:         "no series",
	url:           "~charmers/juju-gui",
	expectStatus:  http.StatusForbidden,
	expectMessage: "series not specified in url or charm metadata",
	expectCode:    params.ErrEntityIdNotAllowed,
}, {
	about: "url series not in metadata",
	url:   "~charmers/precise/juju-gui",
	entity: storetesting.NewCharm(&charm.Meta{
		Series: []string{"trusty"},
	}),
	expectStatus:  http.StatusForbidden,
	expectMessage: `"precise" series not listed in charm metadata`,
	expectCode:    params.ErrEntityIdNotAllowed,
}, {
	about: "bad combination of series",
	url:   "~charmers/juju-gui",
	entity: storetesting.NewCharm(&charm.Meta{
		Series: []string{"precise", "win10"},
	}),
	expectStatus:  http.StatusBadRequest,
	expectMessage: `cannot mix series from ubuntu and windows in single charm`,
	expectCode:    params.ErrInvalidEntity,
}, {
	about: "unknown series",
	url:   "~charmers/juju-gui",
	entity: storetesting.NewCharm(&charm.Meta{
		Series: []string{"precise", "nosuchseries"},
	}),
	expectStatus:  http.StatusBadRequest,
	expectMessage: `unrecognized series "nosuchseries" in metadata`,
	expectCode:    params.ErrInvalidEntity,
}}

func (s *ArchiveSuite) TestPostErrors(c *gc.C) {
	type exoticReader struct {
		io.Reader
	}
	for i, test := range archivePostErrorsTests {
		c.Logf("test %d: %s", i, test.about)
		if test.entity == nil {
			test.entity = storetesting.NewCharm(nil)
		}
		blob, hashSum := getBlob(test.entity)
		body := io.Reader(blob)
		if test.noContentLength {
			// net/http will automatically add a Content-Length header
			// if it sees *strings.Reader, but not if it's a type it doesn't
			// know about.
			body = exoticReader{body}
		}
		path := storeURL(test.url) + "/archive"
		if !test.noHash {
			path += "?hash=" + hashSum
		}
		httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
			Handler: s.srv,
			URL:     path,
			Method:  "POST",
			Header: http.Header{
				"Content-Type": {"application/zip"},
			},
			Body:         body,
			Username:     testUsername,
			Password:     testPassword,
			ExpectStatus: test.expectStatus,
			ExpectBody: params.Error{
				Message: test.expectMessage,
				Code:    test.expectCode,
			},
		})
	}
}

func (s *ArchiveSuite) TestConcurrentUploads(c *gc.C) {
	wordpress := storetesting.Charms.CharmArchive(c.MkDir(), "wordpress")
	f, err := os.Open(wordpress.Path)
	c.Assert(err, gc.Equals, nil)

	var buf bytes.Buffer
	_, err = io.Copy(&buf, f)
	c.Assert(err, gc.Equals, nil)

	hash, _ := hashOf(bytes.NewReader(buf.Bytes()))

	srv := httptest.NewServer(s.srv)
	defer srv.Close()

	// Our strategy for testing concurrent uploads is as follows: We
	// repeat uploading a bunch of simultaneous uploads to the same
	// charm. Each upload should succeed. We make sure that all replies are
	// like this.

	tries := make(chan struct{})

	// upload performs one upload of the testing charm.
	// It sends the response body on the errorBodies channel when
	// it finds an error response.
	upload := func() {
		c.Logf("uploading")
		body := bytes.NewReader(buf.Bytes())
		url := srv.URL + storeURL("~charmers/precise/wordpress/archive?hash="+hash)
		req, err := http.NewRequest("POST", url, body)
		c.Assert(err, gc.Equals, nil)
		req.Header.Set("Content-Type", "application/zip")
		req.SetBasicAuth(testUsername, testPassword)
		resp, err := http.DefaultClient.Do(req)
		if !c.Check(err, gc.Equals, nil) {
			return
		}
		defer resp.Body.Close()
		c.Check(resp.StatusCode, gc.Equals, http.StatusOK)
		tries <- struct{}{}
	}

	// The try loop continues concurrently uploading
	// charms until it is told to stop (by closing the try
	// channel). It then signals that it has terminated
	// by closing errorBodies.
	try := make(chan struct{})
	go func(try chan struct{}) {
		for range try {
			var wg sync.WaitGroup
			for p := 0; p < 5; p++ {
				wg.Add(1)
				go func() {
					upload()
					wg.Done()
				}()
			}
			wg.Wait()
		}
		close(tries)
	}(try)

	count := 0
loop:
	for {
		select {
		case _, ok := <-tries:
			if !ok {
				// The try loop has terminated,
				// so we need to stop too.
				break loop
			}
		case try <- struct{}{}:
			if count++; count > 10 {
				close(try)
				try = nil
			}
		}
	}
}

func (s *ArchiveSuite) TestPostCharm(c *gc.C) {
	s.idmServer.SetDefaultUser("charmers")

	// A charm that did not exist before should get revision 0.
	s.assertUploadCharm(c, "POST", newResolvedURL("~charmers/precise/wordpress-0", -1), "wordpress", nil)

	// Subsequent charm uploads should increment the revision by 1.
	s.assertUploadCharm(c, "POST", newResolvedURL("~charmers/precise/wordpress-1", -1), "mysql", nil)

	// Subsequent charm uploads should increment the revision by 1.
	s.assertUploadCharm(c, "POST", newResolvedURL("~charmers/precise/wordpress-2", -1), "wordpress", nil)

	// Retrieving the unpublished version returns the latest charm.
	rec := httptesting.DoRequest(c, httptesting.DoRequestParams{
		Handler: s.srv,
		URL:     storeURL("~charmers/wordpress/archive?channel=unpublished"),
		Do:      bakeryDo(nil),
	})
	c.Assert(rec.Code, gc.Equals, http.StatusOK, gc.Commentf("body: %s", rec.Body))
	c.Assert(rec.Header().Get(params.EntityIdHeader), gc.Equals, "cs:~charmers/precise/wordpress-2")
}

func (s *ArchiveSuite) TestPostCurrentVersion(c *gc.C) {
	s.assertUploadCharm(c, "POST", newResolvedURL("~charmers/precise/wordpress-0", -1), "wordpress", nil)

	// Subsequent charm uploads should not increment the revision by 1.
	s.assertUploadCharm(c, "POST", newResolvedURL("~charmers/precise/wordpress-0", -1), "wordpress", nil)
}

func (s *ArchiveSuite) TestPostMultiSeriesCharm(c *gc.C) {
	// A charm that did not exist before should get revision 0.
	s.assertUploadCharm(c, "POST", newResolvedURL("~charmers/juju-gui-0", -1), "multi-series", nil)
}

func (s *ArchiveSuite) TestPostMultiSeriesCharmRevisionAfterAllSingleSeriesOnes(c *gc.C) {
	// Create some single series versions of the charm
	s.assertUploadCharm(c, "PUT", newResolvedURL("~charmers/vivid/juju-gui-1", -1), "mysql", nil)
	s.assertUploadCharm(c, "PUT", newResolvedURL("~charmers/trusty/juju-gui-12", -1), "mysql", nil)
	s.assertUploadCharm(c, "PUT", newResolvedURL("~charmers/precise/juju-gui-44", -1), "mysql", nil)

	// Check that the new multi-series revision takes the a revision
	// number larger than the largest of all the single series
	// revisions.
	s.assertUploadCharm(c, "POST", newResolvedURL("~charmers/juju-gui-45", -1), "multi-series", nil)
}

func (s *ArchiveSuite) TestPostMultiSeriesPromulgatedRevisionAfterAllSingleSeriesOnes(c *gc.C) {
	// Create some single series versions of the charm
	s.assertUploadCharm(c, "PUT", newResolvedURL("~charmers/vivid/juju-gui-1", 0), "mysql", nil)
	s.assertUploadCharm(c, "PUT", newResolvedURL("~charmers/trusty/juju-gui-12", 9), "mysql", nil)
	s.assertUploadCharm(c, "PUT", newResolvedURL("~charmers/precise/juju-gui-44", 33), "mysql", nil)

	// Check that the new multi-series promulgated revision has
	// a revision number larger than the largest of all the single
	// series revisions.
	s.assertUploadCharm(c, "POST", newResolvedURL("~charmers/juju-gui-45", 34), "multi-series", nil)
}

func (s *ArchiveSuite) TestPostSingleSeriesCharmWhenMultiSeriesVersionExists(c *gc.C) {
	s.assertUploadCharm(c, "POST", newResolvedURL("~charmers/juju-gui-0", -1), "multi-series", nil)

	s.assertUploadCharmError(
		c,
		"POST",
		charm.MustParseURL("~charmers/saucy/juju-gui-0"),
		nil,
		"wordpress",
		nil,
		http.StatusForbidden,
		params.Error{
			Message: "charm name duplicates multi-series charm name cs:~charmers/juju-gui-0",
			Code:    params.ErrEntityIdNotAllowed,
		},
	)
}

func (s *ArchiveSuite) TestPutSingleSeriesCharmWhenMultiSeriesVersionExists(c *gc.C) {
	s.assertUploadCharm(c, "POST", newResolvedURL("~charmers/juju-gui-0", -1), "multi-series", nil)

	s.assertUploadCharmError(
		c,
		"PUT",
		charm.MustParseURL("~charmers/saucy/juju-gui-1"),
		nil,
		"wordpress",
		nil,
		http.StatusForbidden,
		params.Error{
			Message: "charm name duplicates multi-series charm name cs:~charmers/juju-gui-0",
			Code:    params.ErrEntityIdNotAllowed,
		},
	)
}

func (s *ArchiveSuite) TestPostKubernetesCharm(c *gc.C) {
	// A charm that did not exist before should get revision 0.
	s.assertUploadCharm(c, "POST", newResolvedURL("~charmers/juju-gui-0", -1), "kubernetes", nil)
}

func (s *ArchiveSuite) TestPutCharmWithChannel(c *gc.C) {
	s.assertUploadCharm(c, "PUT", newResolvedURL("~charmers/precise/juju-gui-0", -1), "wordpress", []params.Channel{params.EdgeChannel})
	s.assertUploadCharm(c, "PUT", newResolvedURL("~charmers/precise/juju-gui-1", -1), "wordpress", []params.Channel{params.StableChannel})
	s.assertUploadCharm(c, "PUT", newResolvedURL("~charmers/precise/juju-gui-2", -1), "wordpress", []params.Channel{params.StableChannel, params.EdgeChannel})
}

func (s *ArchiveSuite) TestPutCharmWithInvalidChannel(c *gc.C) {
	s.assertUploadCharmError(
		c,
		"PUT",
		charm.MustParseURL("~charmers/saucy/juju-gui-0"),
		nil,
		"wordpress",
		[]params.Channel{params.EdgeChannel, "bad"},
		http.StatusBadRequest,
		params.Error{
			Message: `invalid channel "bad" specified in request`,
			Code:    params.ErrBadRequest,
		},
	)
	s.assertUploadCharmError(
		c,
		"PUT",
		charm.MustParseURL("~charmers/saucy/juju-gui-0"),
		nil,
		"wordpress",
		[]params.Channel{params.UnpublishedChannel},
		http.StatusBadRequest,
		params.Error{
			Message: `cannot put entity into channel "unpublished"`,
			Code:    params.ErrBadRequest,
		},
	)
}

func (s *ArchiveSuite) TestPutMultiseriesCharm(c *gc.C) {
	s.assertUploadCharm(c, "PUT", newResolvedURL("~charmers/juju-gui-2", -1), "multi-series", nil)
}

func (s *ArchiveSuite) TestPutBundle(c *gc.C) {
	// Upload the required charms.
	for _, rurl := range []*router.ResolvedURL{
		newResolvedURL("cs:~charmers/utopic/mysql-42", 42),
		newResolvedURL("cs:~charmers/utopic/wordpress-47", 47),
		newResolvedURL("cs:~charmers/utopic/logging-1", 1),
	} {
		err := s.store.AddCharmWithArchive(rurl, storetesting.Charms.CharmArchive(c.MkDir(), rurl.URL.Name))
		c.Assert(err, gc.Equals, nil)
		err = s.store.Publish(rurl, nil, params.StableChannel)
		c.Assert(err, gc.Equals, nil)
	}

	s.assertUploadBundle(c, "PUT", newResolvedURL("~charmers/bundle/wordpress-simple-2", -1), "wordpress-simple")
}

func (s *ArchiveSuite) TestPutCharm(c *gc.C) {
	s.assertUploadCharm(
		c,
		"PUT",
		newResolvedURL("~charmers/precise/wordpress-3", 3),
		"wordpress",
		nil,
	)

	s.assertUploadCharm(
		c,
		"PUT",
		newResolvedURL("~charmers/precise/wordpress-1", -1),
		"wordpress",
		nil,
	)

	// Check that we get a duplicate-upload error if we try to
	// upload to the same revision again.
	s.assertUploadCharmError(
		c,
		"PUT",
		charm.MustParseURL("~charmers/precise/wordpress-3"),
		nil,
		"mysql",
		nil,
		http.StatusInternalServerError,
		params.Error{
			Message: "duplicate upload",
			Code:    params.ErrDuplicateUpload,
		},
	)

	// Check we get an error if promulgated url already uploaded.
	s.assertUploadCharmError(
		c,
		"PUT",
		charm.MustParseURL("~charmers/precise/wordpress-4"),
		charm.MustParseURL("precise/wordpress-3"),
		"wordpress",
		nil,
		http.StatusInternalServerError,
		params.Error{
			Message: "duplicate upload",
			Code:    params.ErrDuplicateUpload,
		},
	)

	// Check we get an error if promulgated url has user.
	s.assertUploadCharmError(
		c,
		"PUT",
		charm.MustParseURL("~charmers/precise/wordpress-4"),
		charm.MustParseURL("~charmers/precise/wordpress-4"),
		"mysql",
		nil,
		http.StatusBadRequest,
		params.Error{
			Message: "promulgated URL cannot have a user",
			Code:    params.ErrBadRequest,
		},
	)

	// Check we get an error if promulgated url has different name.
	s.assertUploadCharmError(
		c,
		"PUT",
		charm.MustParseURL("~charmers/precise/wordpress-4"),
		charm.MustParseURL("precise/mysql-4"),
		"mysql",
		nil,
		http.StatusBadRequest,
		params.Error{
			Message: "promulgated URL has incorrect charm name",
			Code:    params.ErrBadRequest,
		},
	)
}

func (s *ArchiveSuite) TestPostBundle(c *gc.C) {
	// Upload the required charms.
	for _, rurl := range []*router.ResolvedURL{
		newResolvedURL("cs:~charmers/utopic/mysql-42", 42),
		newResolvedURL("cs:~charmers/utopic/wordpress-47", 47),
		newResolvedURL("cs:~charmers/utopic/logging-1", 1),
	} {
		err := s.store.AddCharmWithArchive(rurl, storetesting.Charms.CharmArchive(c.MkDir(), rurl.URL.Name))
		c.Assert(err, gc.Equals, nil)
		err = s.store.Publish(rurl, nil, params.StableChannel)
		c.Assert(err, gc.Equals, nil)
	}

	// A bundle that did not exist before should get revision 0.
	s.assertUploadBundle(c, "POST", newResolvedURL("~charmers/bundle/wordpress-simple-0", -1), "wordpress-simple")

	// Subsequent bundle uploads should increment the
	// revision by 1.
	s.assertUploadBundle(c, "POST", newResolvedURL("~charmers/bundle/wordpress-simple-1", -1), "wordpress-with-logging")

	// Uploading the same archive twice should not increment the revision...
	s.assertUploadBundle(c, "POST", newResolvedURL("~charmers/bundle/wordpress-simple-1", -1), "wordpress-with-logging")

	// ... but uploading an archive used by a previous revision should.
	s.assertUploadBundle(c, "POST", newResolvedURL("~charmers/bundle/wordpress-simple-2", -1), "wordpress-simple")
}

func (s *ArchiveSuite) TestPostHashMismatch(c *gc.C) {
	content := []byte("some content")
	hash, _ := hashOf(bytes.NewReader(content))

	// Corrupt the content.
	copy(content, "bogus")
	path := fmt.Sprintf("~charmers/precise/wordpress/archive?hash=%s", hash)
	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler: s.srv,
		URL:     storeURL(path),
		Method:  "POST",
		Header: http.Header{
			"Content-Type": {"application/zip"},
		},
		Body:         bytes.NewReader(content),
		Username:     testUsername,
		Password:     testPassword,
		ExpectStatus: http.StatusInternalServerError,
		ExpectBody: params.Error{
			Message: "cannot put archive blob: hash mismatch",
		},
	})
}

func (s *ArchiveSuite) TestPostEntityClearsCanIngest(c *gc.C) {
	id := newResolvedURL("~charmers/precise/juju-gui-0", -1)
	s.assertUploadCharm(c, "PUT", id, "wordpress", nil)
	s.setPublic(c, id)

	// Sanity check that can-ingest is false after the initial PUT.
	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler: s.srv,
		URL:     storeURL(id.URL.Path() + "/meta/can-ingest"),
		ExpectBody: params.CanIngestResponse{
			CanIngest: true,
		},
	})
	id1 := *id
	id1.URL.Revision = 1
	s.assertUploadCharm(c, "POST", &id1, "mysql", nil)

	// Uploading with POST should have set can-ingest to false.
	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler: s.srv,
		URL:     storeURL(id.URL.Path() + "/meta/can-ingest"),
		ExpectBody: params.CanIngestResponse{
			CanIngest: false,
		},
	})
}

func (s *ArchiveSuite) TestPostEntityWithIngestDoesNotClearCanIngest(c *gc.C) {
	id := newResolvedURL("~charmers/precise/juju-gui-0", -1)
	s.assertUploadCharm(c, "PUT", id, "wordpress", nil)
	s.setPublic(c, id)

	// Sanity check that can-ingest is false after the initial PUT.
	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler: s.srv,
		URL:     storeURL(id.URL.Path() + "/meta/can-ingest"),
		ExpectBody: params.CanIngestResponse{
			CanIngest: true,
		},
	})
	id1 := *id
	id1.URL.Revision = 1
	ch := storetesting.Charms.CharmArchive(c.MkDir(), "mysql")
	s.assertUpload(c, uploadParams{
		method:   "POST",
		id:       &id1,
		fileName: ch.Path,
		ingest:   true,
	})

	// Uploading with POST but with the ingest flag should leave can-ingest as it is.
	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler: s.srv,
		URL:     storeURL(id.URL.Path() + "/meta/can-ingest"),
		ExpectBody: params.CanIngestResponse{
			CanIngest: true,
		},
	})
}

func invalidZip() io.ReadSeeker {
	return strings.NewReader("invalid zip content")
}

func (s *ArchiveSuite) TestPostInvalidCharmZip(c *gc.C) {
	s.assertCannotUpload(c, "~charmers/precise/wordpress", invalidZip(), http.StatusBadRequest, params.ErrInvalidEntity, "cannot read charm archive: zip: not a valid zip file")
}

func (s *ArchiveSuite) TestPostInvalidBundleZip(c *gc.C) {
	s.assertCannotUpload(c, "~charmers/bundle/wordpress", invalidZip(), http.StatusBadRequest, params.ErrInvalidEntity, "cannot read bundle archive: zip: not a valid zip file")
}

var postInvalidCharmMetadataTests = []struct {
	about       string
	spec        charmtesting.CharmSpec
	expectError string
}{{
	about: "bad provider relation name",
	spec: charmtesting.CharmSpec{
		Meta: `
name: foo
summary: bar
description: d
provides:
    relation-name:
        interface: baz
`,
	},
	expectError: "relation relation-name has almost certainly not been changed from the template",
}, {
	about: "bad provider interface name",
	spec: charmtesting.CharmSpec{
		Meta: `
name: foo
summary: bar
description: d
provides:
    baz:
        interface: interface-name
`,
	},
	expectError: "interface interface-name in relation baz has almost certainly not been changed from the template",
}, {
	about: "bad requirer relation name",
	spec: charmtesting.CharmSpec{
		Meta: `
name: foo
summary: bar
description: d
requires:
    relation-name:
        interface: baz
`,
	},
	expectError: "relation relation-name has almost certainly not been changed from the template",
}, {
	about: "bad requirer interface name",
	spec: charmtesting.CharmSpec{
		Meta: `
name: foo
summary: bar
description: d
requires:
    baz:
        interface: interface-name
`,
	},
	expectError: "interface interface-name in relation baz has almost certainly not been changed from the template",
}, {
	about: "bad peer relation name",
	spec: charmtesting.CharmSpec{
		Meta: `
name: foo
summary: bar
description: d
peers:
    relation-name:
        interface: baz
`,
	},
	expectError: "relation relation-name has almost certainly not been changed from the template",
}, {
	about: "bad peer interface name",
	spec: charmtesting.CharmSpec{
		Meta: `
name: foo
summary: bar
description: d
peers:
    baz:
        interface: interface-name
`,
	},
	expectError: "interface interface-name in relation baz has almost certainly not been changed from the template",
}}

func (s *ArchiveSuite) TestPostInvalidCharmMetadata(c *gc.C) {
	for i, test := range postInvalidCharmMetadataTests {
		c.Logf("test %d: %s", i, test.about)
		ch := charmtesting.NewCharm(c, test.spec)
		r := bytes.NewReader(ch.ArchiveBytes())
		s.assertCannotUpload(c, "~charmers/trusty/wordpress", r, http.StatusBadRequest, params.ErrInvalidEntity, test.expectError)
	}
}

func (s *ArchiveSuite) TestPostInvalidBundleData(c *gc.C) {
	path := storetesting.Charms.BundleArchivePath(c.MkDir(), "bad")
	f, err := os.Open(path)
	c.Assert(err, gc.Equals, nil)
	defer f.Close()
	// Here we exercise both bundle internal verification (bad relation) and
	// validation with respect to charms (wordpress and mysql are missing).
	expectErr := `bundle verification failed: [` +
		`"application \"mysql\" refers to non-existent charm \"cs:mysql\"",` +
		`"application \"wordpress\" refers to non-existent charm \"cs:wordpress\"",` +
		`"relation [\"foo:db\" \"mysql:server\"] refers to application \"foo\" not defined in this bundle"]`
	s.assertCannotUpload(c, "~charmers/bundle/wordpress", f, http.StatusBadRequest, params.ErrInvalidEntity, expectErr)
}

func (s *ArchiveSuite) TestUploadOfCurrentCharmReadsFully(c *gc.C) {
	s.assertUploadCharm(c, "POST", newResolvedURL("~charmers/precise/wordpress-0", -1), "wordpress", nil)

	ch := storetesting.Charms.CharmArchive(c.MkDir(), "wordpress")
	f, err := os.Open(ch.Path)
	c.Assert(err, gc.Equals, nil)
	defer f.Close()

	// Calculate blob hashes.
	hash := blobstore.NewHash()
	_, err = io.Copy(hash, f)
	c.Assert(err, gc.Equals, nil)
	hashSum := fmt.Sprintf("%x", hash.Sum(nil))

	// Simulate upload of current version
	h := s.handler(c)
	defer h.Close()
	b := bytes.NewBuffer([]byte("test body"))
	r, err := http.NewRequest("POST", "/~charmers/precise/wordpress/archive?hash="+hashSum, b)
	c.Assert(err, gc.Equals, nil)
	r.Header.Set("Content-Type", "application/zip")
	r.SetBasicAuth(testUsername, testPassword)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	httptesting.AssertJSONResponse(
		c,
		rec,
		http.StatusOK,
		params.ArchiveUploadResponse{
			Id: charm.MustParseURL("~charmers/precise/wordpress-0"),
		},
	)
	c.Assert(b.Len(), gc.Equals, 0)
}

func (s *ArchiveSuite) assertCannotUpload(c *gc.C, id string, content io.ReadSeeker, httpStatus int, errorCode params.ErrorCode, errorMessage string) {
	hash, size := hashOf(content)
	_, err := content.Seek(0, 0)
	c.Assert(err, gc.Equals, nil)

	path := fmt.Sprintf("%s/archive?hash=%s", id, hash)
	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler:       s.srv,
		URL:           storeURL(path),
		Method:        "POST",
		ContentLength: size,
		Header: http.Header{
			"Content-Type": {"application/zip"},
		},
		Body:         content,
		Username:     testUsername,
		Password:     testPassword,
		ExpectStatus: httpStatus,
		ExpectBody: params.Error{
			Message: errorMessage,
			Code:    errorCode,
		},
	})

	// TODO(rog) check that the uploaded blob has been deleted,
	// by checking that no new blobs have been added to the blob store.
}

// assertUploadCharm uploads the testing charm with the given name
// through the API. The URL must hold the expected revision
// that the charm will be given when uploaded.
func (s *commonArchiveSuite) assertUploadCharm(c *gc.C, method string, url *router.ResolvedURL, charmName string, chans []params.Channel) *charm.CharmArchive {
	ch := storetesting.Charms.CharmArchive(c.MkDir(), charmName)
	id, size := s.assertUpload(c, uploadParams{
		method:   method,
		id:       url,
		fileName: ch.Path,
		chans:    chans,
	})
	s.assertEntityInfo(c, entityInfo{
		Id: id,
		Meta: entityMetaInfo{
			ArchiveSize:  &params.ArchiveSizeResponse{Size: size},
			CharmMeta:    ch.Meta(),
			CharmConfig:  ch.Config(),
			CharmActions: ch.Actions(),
		},
	})
	return ch
}

// assertUploadBundle uploads the testing bundle with the given name
// through the API. The URL must hold the expected revision
// that the bundle will be given when uploaded.
func (s *commonArchiveSuite) assertUploadBundle(c *gc.C, method string, url *router.ResolvedURL, bundleName string) {
	path := storetesting.Charms.BundleArchivePath(c.MkDir(), bundleName)
	b, err := charm.ReadBundleArchive(path)
	c.Assert(err, gc.Equals, nil)
	id, size := s.assertUpload(c, uploadParams{
		method:   method,
		id:       url,
		fileName: path,
	})
	s.assertEntityInfo(c, entityInfo{
		Id: id,
		Meta: entityMetaInfo{
			ArchiveSize: &params.ArchiveSizeResponse{Size: size},
			BundleMeta:  b.Data(),
		},
	},
	)
}

type uploadParams struct {
	method   string
	id       *router.ResolvedURL
	fileName string
	chans    []params.Channel
	ingest   bool
}

func (s *commonArchiveSuite) assertUpload(c *gc.C, p uploadParams) (id *charm.URL, size int64) {
	f, err := os.Open(p.fileName)
	c.Assert(err, gc.Equals, nil)
	defer f.Close()

	// Calculate blob hashes.
	hash := blobstore.NewHash()
	hash256 := sha256.New()
	size, err = io.Copy(io.MultiWriter(hash, hash256), f)
	c.Assert(err, gc.Equals, nil)
	hashSum := fmt.Sprintf("%x", hash.Sum(nil))
	hash256Sum := fmt.Sprintf("%x", hash256.Sum(nil))
	_, err = f.Seek(0, 0)
	c.Assert(err, gc.Equals, nil)

	uploadURL := p.id.URL
	if p.method == "POST" {
		uploadURL.Revision = -1
	}

	path := fmt.Sprintf("%s/archive?hash=%s", uploadURL.Path(), hashSum)
	for _, c := range p.chans {
		path += fmt.Sprintf("&channel=%s", c)
	}
	if p.ingest {
		path += "&ingest=1"
	}
	expectId := uploadURL.WithRevision(p.id.URL.Revision)
	expectedPromulgatedId := p.id.PromulgatedURL()
	if expectedPromulgatedId != nil {
		path += fmt.Sprintf("&promulgated=%s", expectedPromulgatedId.String())
	}
	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler:       s.srv,
		URL:           storeURL(path),
		Method:        p.method,
		ContentLength: size,
		Header: http.Header{
			"Content-Type": {"application/zip"},
		},
		Body:     f,
		Username: testUsername,
		Password: testPassword,
		ExpectBody: params.ArchiveUploadResponse{
			Id:            expectId,
			PromulgatedId: expectedPromulgatedId,
		},
	})

	// Make sure that the entity can be found in
	// all the channels we tried to publish it in
	// and not in any others.
	expectChans := map[params.Channel]bool{
		params.UnpublishedChannel: true,
	}
	for _, ch := range p.chans {
		expectChans[ch] = true
	}
	for _, ch := range params.OrderedChannels {
		_, err := s.store.FindBestEntity(&p.id.URL, ch, nil)
		if expectChans[ch] {
			c.Assert(err, gc.Equals, nil)
		} else {
			c.Assert(err, gc.NotNil)
			c.Assert(errgo.Cause(err), gc.Equals, params.ErrNotFound)
		}
	}

	entity, err := s.store.FindEntity(p.id, nil)
	c.Assert(err, gc.Equals, nil)
	c.Assert(entity.BlobHash, gc.Equals, hashSum)
	if p.id.URL.Series != "" {
		c.Assert(entity.BlobHash256, gc.Equals, hash256Sum)
	}
	c.Assert(entity.PromulgatedURL, gc.DeepEquals, p.id.PromulgatedURL())

	delete(expectChans, params.UnpublishedChannel)
	if len(expectChans) == 0 {
		c.Assert(entity.Published, gc.IsNil)
	} else {
		c.Assert(entity.Published, gc.DeepEquals, expectChans)
	}

	// Test that the expected entry has been created
	// in the blob store.
	r, _, err := s.store.BlobStore.Open(entity.BlobHash, nil)
	c.Assert(err, gc.Equals, nil)
	r.Close()

	return expectId, size
}

// assertUploadCharmError attempts to upload the testing charm with the
// given name through the API, checking that the attempt fails with the
// specified error. The URL must hold the expected revision that the
// charm will be given when uploaded.
func (s *ArchiveSuite) assertUploadCharmError(c *gc.C, method string, url, purl *charm.URL, charmName string, chans []params.Channel, expectStatus int, expectBody interface{}) {
	ch := storetesting.Charms.CharmDir(charmName)
	s.assertUploadError(c, method, url, purl, ch, chans, expectStatus, expectBody)
}

// assertUploadError asserts that we get an error when uploading
// the contents of the given file to the given url and promulgated URL.
// The reason this method does not take a *router.ResolvedURL
// is so that we can test what happens when an inconsistent promulgated URL
// is passed in.
func (s *ArchiveSuite) assertUploadError(c *gc.C, method string, url, purl *charm.URL, entity charmstore.ArchiverTo, chans []params.Channel, expectStatus int, expectBody interface{}) {
	blob, hashSum := getBlob(entity)

	uploadURL := *url
	if method == "POST" {
		uploadURL.Revision = -1
	}

	path := fmt.Sprintf("%s/archive?hash=%s", uploadURL.Path(), hashSum)
	for _, c := range chans {
		path += fmt.Sprintf("&channel=%s", c)
	}
	if purl != nil {
		path += fmt.Sprintf("&promulgated=%s", purl.String())
	}
	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler:       s.srv,
		URL:           storeURL(path),
		Method:        method,
		ContentLength: int64(blob.Len()),
		Header: http.Header{
			"Content-Type": {"application/zip"},
		},
		Body:         blob,
		Username:     testUsername,
		Password:     testPassword,
		ExpectStatus: expectStatus,
		ExpectBody:   expectBody,
	})
}

// getBlob returns the contents and blob checksum of the given entity.
func getBlob(entity charmstore.ArchiverTo) (blob *bytes.Buffer, hash string) {
	blob = new(bytes.Buffer)
	err := entity.ArchiveTo(blob)
	if err != nil {
		panic(err)
	}
	h := blobstore.NewHash()
	h.Write(blob.Bytes())
	hash = fmt.Sprintf("%x", h.Sum(nil))
	return blob, hash
}

var archiveFileErrorsTests = []struct {
	about         string
	path          string
	expectStatus  int
	expectMessage string
	expectCode    params.ErrorCode
}{{
	about:         "entity not found",
	path:          "~charmers/trusty/no-such-42/archive/icon.svg",
	expectStatus:  http.StatusNotFound,
	expectMessage: `no matching charm or bundle for cs:~charmers/trusty/no-such-42`,
	expectCode:    params.ErrNotFound,
}, {
	about:         "directory listing",
	path:          "~charmers/utopic/wordpress-0/archive/hooks",
	expectStatus:  http.StatusForbidden,
	expectMessage: "directory listing not allowed",
	expectCode:    params.ErrForbidden,
}, {
	about:         "file not found",
	path:          "~charmers/utopic/wordpress-0/archive/no-such",
	expectStatus:  http.StatusNotFound,
	expectMessage: `file "no-such" not found in the archive`,
	expectCode:    params.ErrNotFound,
}, {
	about:         "no permissions",
	path:          "~charmers/utopic/mysql-0/archive/metadata.yaml",
	expectStatus:  http.StatusUnauthorized,
	expectMessage: `access denied for user "bob"`,
	expectCode:    params.ErrUnauthorized,
}}

func (s *ArchiveSuite) TestArchiveFileErrors(c *gc.C) {
	s.addPublicCharmFromRepo(c, "wordpress", newResolvedURL("cs:~charmers/utopic/wordpress-0", 0))
	id, _ := s.addPublicCharmFromRepo(c, "mysql", newResolvedURL("cs:~charmers/utopic/mysql-0", 0))
	err := s.store.SetPerms(&id.URL, "stable.read", "no-one")
	c.Assert(err, gc.Equals, nil)
	s.idmServer.SetDefaultUser("bob")
	for i, test := range archiveFileErrorsTests {
		c.Logf("test %d: %s", i, test.about)
		httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
			Handler:      s.srv,
			URL:          storeURL(test.path),
			Do:           bakeryDo(nil),
			ExpectStatus: test.expectStatus,
			ExpectBody: params.Error{
				Message: test.expectMessage,
				Code:    test.expectCode,
			},
		})
	}
}

func (s *ArchiveSuite) TestArchiveFileGet(c *gc.C) {
	ch := storetesting.Charms.CharmArchive(c.MkDir(), "all-hooks")
	id := newResolvedURL("cs:~charmers/utopic/all-hooks-0", 0)
	s.addPublicCharm(c, ch, id)
	zipFile, err := zip.OpenReader(ch.Path)
	c.Assert(err, gc.Equals, nil)
	defer zipFile.Close()

	// Check a file in the root directory.
	s.assertArchiveFileContents(c, zipFile, "~charmers/utopic/all-hooks-0/archive/metadata.yaml")
	// Check a file in a subdirectory.
	s.assertArchiveFileContents(c, zipFile, "~charmers/utopic/all-hooks-0/archive/hooks/install")
}

// assertArchiveFileContents checks that the response returned by the
// serveArchiveFile endpoint is correct for the given archive and URL path.
func (s *ArchiveSuite) assertArchiveFileContents(c *gc.C, zipFile *zip.ReadCloser, path string) {
	// For example: trusty/django/archive/hooks/install -> hooks/install.
	filePath := strings.SplitN(path, "/archive/", 2)[1]

	// Retrieve the expected bytes.
	var expectBytes []byte
	for _, file := range zipFile.File {
		if file.Name == filePath {
			r, err := file.Open()
			c.Assert(err, gc.Equals, nil)
			defer r.Close()
			expectBytes, err = ioutil.ReadAll(r)
			c.Assert(err, gc.Equals, nil)
			break
		}
	}
	c.Assert(expectBytes, gc.Not(gc.HasLen), 0)

	// Make the request.
	url := storeURL(path)
	rec := httptesting.DoRequest(c, httptesting.DoRequestParams{
		Handler: s.srv,
		URL:     url,
	})

	// Ensure the response is what we expect.
	c.Assert(rec.Code, gc.Equals, http.StatusOK)
	c.Assert(rec.Body.Bytes(), gc.DeepEquals, expectBytes)
	headers := rec.Header()
	c.Assert(headers.Get("Content-Length"), gc.Equals, strconv.Itoa(len(expectBytes)))
	// We only have text files in the charm repository used for tests.
	c.Assert(headers.Get("Content-Type"), gc.Equals, "text/plain; charset=utf-8")
	assertCacheControl(c, rec.Header(), true)
}

func (s *ArchiveSuite) TestDelete(c *gc.C) {
	// Add a charm to the database (including the archive).
	id, _ := s.addPublicCharm(c, storetesting.NewCharm(nil), newResolvedURL("~charmers/utopic/mysql-42", -1))
	// Add a second charm so that we're not trying to delete the last revision.
	s.addPublicCharm(c, storetesting.NewCharm(nil), newResolvedURL("~charmers/utopic/mysql-43", -1))

	// Retrieve the corresponding entity.
	var entity mongodoc.Entity
	err := s.store.DB.Entities().FindId(&id.URL).Select(bson.D{{"blobhash", 1}}).One(&entity)
	c.Assert(err, gc.Equals, nil)

	s.doAsUser("charmers", func() {
		// Delete the charm using the API.
		httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
			Handler: s.srv,
			Do:      bakeryDo(nil),
			URL:     storeURL(id.URL.Path() + "/archive"),
			Method:  "DELETE",
		})
	})

	// The entity has been deleted.
	count, err := s.store.DB.Entities().FindId(&id.URL).Count()
	c.Assert(err, gc.Equals, nil)
	c.Assert(count, gc.Equals, 0)
}

func (s *ArchiveSuite) TestDeleteSpecificCharm(c *gc.C) {
	// Add a couple of charms to the database.
	for _, id := range []string{"~charmers/trusty/mysql-42", "~charmers/utopic/mysql-42", "~charmers/utopic/mysql-47"} {
		err := s.store.AddCharmWithArchive(
			newResolvedURL(id, -1),
			storetesting.Charms.CharmArchive(c.MkDir(), "mysql"))
		c.Assert(err, gc.Equals, nil)
	}

	s.doAsUser("charmers", func() {
		// Delete the second charm using the API.
		httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
			Handler: s.srv,
			Do:      bakeryDo(nil),
			URL:     storeURL("~charmers/utopic/mysql-42/archive"),
			Method:  "DELETE",
		})
	})

	// The other two charms are still present in the database.
	urls := []*charm.URL{
		charm.MustParseURL("~charmers/trusty/mysql-42"),
		charm.MustParseURL("~charmers/utopic/mysql-47"),
	}
	count, err := s.store.DB.Entities().Find(bson.D{{
		"_id", bson.D{{"$in", urls}},
	}}).Count()
	c.Assert(err, gc.Equals, nil)
	c.Assert(count, gc.Equals, 2)
}

func (s *ArchiveSuite) TestDeleteNotFound(c *gc.C) {
	// Try to delete a non existing charm using the API.
	s.doAsUser("charmers", func() {
		httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
			Handler:      s.srv,
			Do:           bakeryDo(nil),
			URL:          storeURL("~charmers/utopic/no-such-0/archive"),
			Method:       "DELETE",
			ExpectStatus: http.StatusNotFound,
			ExpectBody: params.Error{
				Message: `no matching charm or bundle for cs:~charmers/utopic/no-such-0`,
				Code:    params.ErrNotFound,
			},
		})
	})
}

func (s *ArchiveSuite) TestDeletePublishedRevision(c *gc.C) {
	s.addPublicCharm(c, storetesting.NewCharm(nil), newResolvedURL("~charmers/precise/wordpress-0", -1))
	s.addPublicCharm(c, storetesting.NewCharm(nil), newResolvedURL("~charmers/precise/wordpress-1", -1))
	s.doAsUser("charmers", func() {
		httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
			Handler:      s.srv,
			Do:           bakeryDo(nil),
			URL:          storeURL("~charmers/precise/wordpress-1/archive"),
			Method:       "DELETE",
			ExpectStatus: http.StatusForbidden,
			ExpectBody: params.Error{
				Code:    params.ErrForbidden,
				Message: `cannot delete "cs:~charmers/precise/wordpress-1": cannot delete "cs:~charmers/precise/wordpress-1" because it is the current revision in channels [stable]`,
			},
		})
	})
}

func (s *ArchiveSuite) TestDeleteUnauthorized(c *gc.C) {
	// Add a charm to the database (not including the archive).
	id := "~charmers/utopic/mysql-42"
	url := newResolvedURL(id, -1)
	err := s.store.AddCharmWithArchive(url, storetesting.Charms.CharmArchive(c.MkDir(), "mysql"))
	c.Assert(err, gc.Equals, nil)

	s.doAsUser("bob", func() {
		// Try to delete the charm using the API.
		httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
			Handler:      s.srv,
			Do:           bakeryDo(nil),
			URL:          storeURL(id + "/archive"),
			Method:       "DELETE",
			ExpectStatus: http.StatusUnauthorized,
			ExpectBody: params.Error{
				Code:    params.ErrUnauthorized,
				Message: `access denied for user "bob"`,
			},
		})
	})
}

type basicAuthArchiveSuite struct {
	commonSuite
}

var _ = gc.Suite(&basicAuthArchiveSuite{})

func (s *basicAuthArchiveSuite) TestPostAuthErrors(c *gc.C) {
	s.checkAuthErrors(c, "POST", "~charmers/utopic/django/archive")
}

func (s *basicAuthArchiveSuite) TestDeleteAuthErrors(c *gc.C) {
	err := s.store.AddCharmWithArchive(
		newResolvedURL("~charmers/utopic/django-42", 42),
		storetesting.Charms.CharmArchive(c.MkDir(), "wordpress"),
	)
	c.Assert(err, gc.Equals, nil)
	s.checkAuthErrors(c, "DELETE", "utopic/django-42/archive")
}

func (s *basicAuthArchiveSuite) TestPostErrorReadsFully(c *gc.C) {
	h := s.handler(c)
	defer h.Close()

	b := strings.NewReader("test body")
	r, err := http.NewRequest("POST", "/~charmers/trusty/wordpress/archive", b)
	c.Assert(err, gc.Equals, nil)
	r.Header.Set("Content-Type", "application/zip")
	r.SetBasicAuth(testUsername, testPassword)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	c.Assert(rec.Code, gc.Equals, http.StatusBadRequest)
	c.Assert(b.Len(), gc.Equals, 0)
}

func (s *basicAuthArchiveSuite) TestPostAuthErrorReadsFully(c *gc.C) {
	h := s.handler(c)
	defer h.Close()
	b := strings.NewReader("test body")
	r, err := http.NewRequest("POST", "/~charmers/trusty/wordpress/archive", b)
	c.Assert(err, gc.Equals, nil)
	r.Header.Set("Content-Type", "application/zip")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	c.Assert(rec.Code, gc.Equals, http.StatusUnauthorized)
	c.Assert(b.Len(), gc.Equals, 0)
}

var archiveAuthErrorsTests = []struct {
	about         string
	header        http.Header
	username      string
	password      string
	expectMessage string
}{{
	about:         "no credentials",
	expectMessage: "authentication failed: missing HTTP auth header",
}, {
	about: "invalid encoding",
	header: http.Header{
		"Authorization": {"Basic not-a-valid-base64"},
	},
	expectMessage: "authentication failed: invalid HTTP auth encoding",
}, {
	about: "invalid header",
	header: http.Header{
		"Authorization": {"Basic " + base64.StdEncoding.EncodeToString([]byte("invalid"))},
	},
	expectMessage: "authentication failed: invalid HTTP auth contents",
}, {
	about:         "invalid credentials",
	username:      "no-such",
	password:      "exterminate!",
	expectMessage: "invalid user name or password",
}}

func (s *basicAuthArchiveSuite) checkAuthErrors(c *gc.C, method, url string) {
	for i, test := range archiveAuthErrorsTests {
		c.Logf("test %d: %s", i, test.about)
		if test.header == nil {
			test.header = http.Header{}
		}
		if method == "POST" {
			test.header.Add("Content-Type", "application/zip")
		}
		httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
			Handler:      s.srv,
			URL:          storeURL(url),
			Method:       method,
			Header:       test.header,
			Username:     test.username,
			Password:     test.password,
			ExpectStatus: http.StatusUnauthorized,
			ExpectBody: params.Error{
				Message: test.expectMessage,
				Code:    params.ErrUnauthorized,
			},
		})
	}
}

// entityInfo holds all the information we want to find
// out about a charm or bundle uploaded to the store.
type entityInfo struct {
	Id   *charm.URL
	Meta entityMetaInfo
}

type entityMetaInfo struct {
	ArchiveSize  *params.ArchiveSizeResponse `json:"archive-size,omitempty"`
	CharmMeta    *charm.Meta                 `json:"charm-metadata,omitempty"`
	CharmConfig  *charm.Config               `json:"charm-config,omitempty"`
	CharmActions *charm.Actions              `json:"charm-actions,omitempty"`
	BundleMeta   *charm.BundleData           `json:"bundle-metadata,omitempty"`
}

func (s *commonArchiveSuite) assertEntityInfo(c *gc.C, expect entityInfo) {
	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler: s.srv,
		URL: storeURL(
			expect.Id.Path() + "/meta/any" +
				"?include=archive-size" +
				"&include=charm-metadata" +
				"&include=charm-config" +
				"&include=charm-actions" +
				"&include=bundle-metadata",
		),
		Username:   testUsername,
		Password:   testPassword,
		ExpectBody: expect,
	})
}

func (s *ArchiveSuite) TestArchiveFileGetHasCORSHeaders(c *gc.C) {
	id := "~charmers/precise/wordpress-0"
	s.assertUploadCharm(c, "POST", newResolvedURL(id, -1), "wordpress", nil)
	rec := httptesting.DoRequest(c, httptesting.DoRequestParams{
		Handler: s.srv,
		URL:     storeURL(fmt.Sprintf("%s/archive/metadata.yaml", id)),
	})
	headers := rec.Header()
	c.Assert(len(headers["Access-Control-Allow-Origin"]), gc.Equals, 1)
	c.Assert(len(headers["Access-Control-Allow-Headers"]), gc.Equals, 1)
	c.Assert(headers["Access-Control-Allow-Origin"][0], gc.Equals, "*")
	c.Assert(headers["Access-Control-Cache-Max-Age"][0], gc.Equals, "600")
	c.Assert(headers["Access-Control-Allow-Headers"][0], gc.Equals, "Bakery-Protocol-Version, Macaroons, X-Requested-With")
}

var getNewPromulgatedRevisionTests = []struct {
	about     string
	id        *charm.URL
	expectRev int
}{{
	about:     "no base entity",
	id:        charm.MustParseURL("cs:~mmouse/trusty/mysql-14"),
	expectRev: -1,
}, {
	about:     "not promulgated",
	id:        charm.MustParseURL("cs:~dduck/trusty/mysql-14"),
	expectRev: -1,
}, {
	about:     "not yet promulgated",
	id:        charm.MustParseURL("cs:~goofy/trusty/mysql-14"),
	expectRev: 0,
}, {
	about:     "existing promulgated",
	id:        charm.MustParseURL("cs:~pluto/trusty/mariadb-14"),
	expectRev: 4,
}, {
	about:     "previous promulgated by different user",
	id:        charm.MustParseURL("cs:~tom/trusty/sed-1"),
	expectRev: 5,
}, {
	about:     "many previous promulgated revisions",
	id:        charm.MustParseURL("cs:~tom/trusty/awk-5"),
	expectRev: 5,
}}

func (s *ArchiveSuite) TestGetNewPromulgatedRevision(c *gc.C) {
	charms := []string{
		"cs:~dduck/trusty/mysql-14",
		"14 cs:~goofy/precise/mysql-14",
		"3 cs:~pluto/trusty/mariadb-5",
		"0 cs:~tom/trusty/sed-0",
		"cs:~jerry/trusty/sed-2",
		"4 cs:~jerry/trusty/sed-3",
		"0 cs:~tom/trusty/awk-0",
		"1 cs:~tom/trusty/awk-1",
		"2 cs:~tom/trusty/awk-2",
		"3 cs:~tom/trusty/awk-3",
		"4 cs:~tom/trusty/awk-4",
	}
	for _, url := range charms {
		ch := storetesting.NewCharm(new(charm.Meta))
		err := s.store.AddCharmWithArchive(mustParseResolvedURL(url), ch)
		c.Assert(err, gc.Equals, nil)
	}
	handler := s.handler(c)
	defer handler.Close()
	for i, test := range getNewPromulgatedRevisionTests {
		c.Logf("%d. %s", i, test.about)
		rev, err := v5.GetNewPromulgatedRevision(handler, test.id)
		c.Assert(err, gc.Equals, nil)
		c.Assert(rev, gc.Equals, test.expectRev)
	}
}

func hashOfBytes(data []byte) string {
	hash := blobstore.NewHash()
	hash.Write(data)
	return fmt.Sprintf("%x", hash.Sum(nil))
}

func hashOf(r io.Reader) (hashSum string, size int64) {
	hash := blobstore.NewHash()
	n, err := io.Copy(hash, r)
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf("%x", hash.Sum(nil)), n
}

func hashOfString(s string) string {
	return hashOfBytes([]byte(s))
}

func rawHash(hash string) []byte {
	bytes, err := hex.DecodeString(hash)
	if err != nil {
		panic(err)
	}
	return bytes
}

// assertCacheControl asserts that the cache control headers are
// appropriately set. The isPublic parameter specifies
// whether the id in the request represents a public charm or bundle.
func assertCacheControl(c *gc.C, h http.Header, isPublic bool) {
	if isPublic {
		seconds := v5.ArchiveCachePublicMaxAge / time.Second
		c.Assert(h.Get("Cache-Control"), gc.Equals, fmt.Sprintf("public, max-age=%d", seconds))
	} else {
		c.Assert(h.Get("Cache-Control"), gc.Equals, "no-cache, must-revalidate")
	}
}

type ArchiveSearchSuite struct {
	commonSuite
}

var _ = gc.Suite(&ArchiveSearchSuite{})

func (s *ArchiveSearchSuite) SetUpSuite(c *gc.C) {
	s.enableES = true
	s.commonSuite.SetUpSuite(c)
}

func (s *ArchiveSearchSuite) SetUpTest(c *gc.C) {
	s.commonSuite.SetUpTest(c)
}

func (s *ArchiveSearchSuite) TestGetSearchUpdate(c *gc.C) {
	for i, id := range []string{"~charmers/bionic/mysql-42", "~who/bionic/mysql-42"} {
		c.Logf("test %d: %s", i, id)
		url := newResolvedURL(id, -1)

		// Add a charm to the database.
		s.addPublicCharm(c, storetesting.NewCharm(nil), url)

		// Download the charm archive using the API.
		rec := httptesting.DoRequest(c, httptesting.DoRequestParams{
			Handler: s.srv,
			URL:     storeURL(id + "/archive"),
		})
		c.Assert(rec.Code, gc.Equals, http.StatusOK)

		// Check that the search record for the entity has been updated.
		stats.CheckSearchTotalDownloads(c, s.store, &url.URL, 1)
	}
}

type ArchiveSuiteWithTerms struct {
	commonArchiveSuite
}

var _ = gc.Suite(&ArchiveSuiteWithTerms{})

func (s *ArchiveSuiteWithTerms) SetUpSuite(c *gc.C) {
	s.commonSuite.SetUpSuite(c)
	s.enableTerms = true
	s.enableIdentity = true
}

func (s *ArchiveSuiteWithTerms) SetUpTest(c *gc.C) {
	s.commonSuite.SetUpTest(c)
	s.idmServer.SetDefaultUser("bob")
}

func (s *ArchiveSuiteWithTerms) TestGetUserHasAgreedToTermsAndConditions(c *gc.C) {
	termsDischargeAccessed := false
	s.dischargeTerms = func(cond, args string) ([]checkers.Caveat, error) {
		termsDischargeAccessed = true
		if cond != "has-agreed" {
			return nil, errgo.New("unexpected condition")
		}
		terms := strings.Fields(args)
		sort.Strings(terms)
		if strings.Join(terms, " ") != "terms-1/1 terms-2/5" {
			return nil, errgo.New("unexpected terms in condition")
		}
		return nil, nil
	}

	client := httpbakery.NewClient()

	ch := storetesting.NewCharm(&charm.Meta{
		Terms: []string{"terms-1/1", "terms-2/5"},
	})
	s.addPublicCharm(c, ch, newResolvedURL("cs:~charmers/precise/terms-0", -1))

	ch1 := storetesting.NewCharm(&charm.Meta{
		Terms: []string{"terms-3/1", "terms-4/5"},
	})
	s.addPublicCharm(c, ch1, newResolvedURL("cs:~charmers/precise/terms1-0", -1))

	s.assertArchiveDownload(
		c,
		"~charmers/precise/terms-0",
		&httptesting.DoRequestParams{
			Do: bakeryDo(client),
		},
		ch.Bytes(),
	)
	c.Assert(termsDischargeAccessed, gc.Equals, true)
	termsDischargeAccessed = false

	s.dischargeTerms = func(cond, args string) ([]checkers.Caveat, error) {
		termsDischargeAccessed = true
		return nil, errgo.New("user has not agreed to specified terms and conditions")
	}

	archiveUrl := storeURL("~charmers/precise/terms1-0/archive")
	httptesting.DoRequest(c, httptesting.DoRequestParams{
		Handler:     s.srv,
		URL:         archiveUrl,
		Do:          bakeryDo(client),
		ExpectError: ".*third party refused discharge: cannot discharge: user has not agreed to specified terms and conditions",
	})
	c.Assert(termsDischargeAccessed, gc.Equals, true)
}

func (s *ArchiveSuiteWithTerms) TestGetArchiveWithBlankMacaroon(c *gc.C) {
	termsDischargeAccessed := false
	s.dischargeTerms = func(cond, args string) ([]checkers.Caveat, error) {
		termsDischargeAccessed = true
		return nil, errgo.New("user has not agreed to specified terms and conditions")
	}

	s.addPublicCharm(c, storetesting.NewCharm(&charm.Meta{
		Terms: []string{"terms-1/1", "terms-2/5"},
	}), newResolvedURL("cs:~charmers/precise/terms-0", -1))

	archiveUrl := storeURL("~charmers/precise/terms-0/archive")

	client := httpbakery.NewClient()
	var gotBody json.RawMessage
	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler:      s.srv,
		URL:          storeURL("macaroon"),
		Do:           bakeryDo(client),
		ExpectStatus: http.StatusOK,
		ExpectBody: httptesting.BodyAsserter(func(c *gc.C, m json.RawMessage) {
			gotBody = m
		}),
	})
	c.Assert(gotBody, gc.NotNil)
	var m macaroon.Macaroon
	err := json.Unmarshal(gotBody, &m)
	c.Assert(err, gc.Equals, nil)

	bClient := httpbakery.NewClient()
	ms, err := bClient.DischargeAll(&m)
	c.Assert(err, gc.Equals, nil)

	u, err := url.Parse("http://127.0.0.1")
	c.Assert(err, gc.Equals, nil)
	err = httpbakery.SetCookie(client.Jar, u, ms)
	c.Assert(err, gc.Equals, nil)

	httptesting.DoRequest(c, httptesting.DoRequestParams{
		Handler:     s.srv,
		URL:         archiveUrl,
		Do:          bakeryDo(client),
		ExpectError: ".*third party refused discharge: cannot discharge: user has not agreed to specified terms and conditions",
	})
	c.Assert(termsDischargeAccessed, gc.Equals, true)
}

func (s *ArchiveSuiteWithTerms) TestGetUserHasNotAgreedToTerms(c *gc.C) {
	s.dischargeTerms = func(_, _ string) ([]checkers.Caveat, error) {
		return nil, errgo.New("user has not agreed to specified terms and conditions")
	}

	s.addPublicCharm(c, storetesting.NewCharm(&charm.Meta{
		Terms: []string{"terms-1/1", "terms-2/5"},
	}), newResolvedURL("cs:~charmers/precise/terms-0", -1))

	archiveUrl := storeURL("~charmers/precise/terms-0/archive")
	httptesting.DoRequest(c, httptesting.DoRequestParams{
		Handler:     s.srv,
		URL:         archiveUrl,
		Do:          bakeryDo(nil),
		ExpectError: ".*third party refused discharge: cannot discharge: user has not agreed to specified terms and conditions",
	})
}

func (s *ArchiveSuiteWithTerms) TestGetIgnoringTermsWithBasicAuth(c *gc.C) {
	s.dischargeTerms = func(_, _ string) ([]checkers.Caveat, error) {
		return nil, errgo.New("user has not agreed to specified terms and conditions")
	}

	ch := storetesting.NewCharm(&charm.Meta{
		Terms: []string{"terms-1/1", "terms-2/5"},
	})
	s.addPublicCharm(c, ch, newResolvedURL("cs:~charmers/precise/terms-0", -1))

	s.assertArchiveDownload(
		c,
		"~charmers/precise/terms-0",
		&httptesting.DoRequestParams{
			Header: basicAuthHeader(testUsername, testPassword),
		},
		ch.Bytes(),
	)
}

func (s *commonSuite) assertArchiveDownload(c *gc.C, id string, extraParams *httptesting.DoRequestParams, archiveBytes []byte) *httptest.ResponseRecorder {
	doParams := httptesting.DoRequestParams{}
	if extraParams != nil {
		doParams = *extraParams
	}
	doParams.Handler = s.srv
	if doParams.URL == "" {
		doParams.URL = storeURL(id + "/archive")
	}
	rec := httptesting.DoRequest(c, doParams)
	c.Assert(rec.Code, gc.Equals, http.StatusOK)

	c.Assert(rec.Body.Bytes(), gc.DeepEquals, archiveBytes)
	c.Assert(rec.Header().Get(params.ContentHashHeader), gc.Equals, hashOfBytes(archiveBytes))
	return rec
}
