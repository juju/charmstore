// Copyright 2016 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package v5_test // import "gopkg.in/juju/charmstore.v5-unstable/internal/v5"

import (
	"crypto/sha512"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/juju/testing/httptesting"
	gc "gopkg.in/check.v1"
	"gopkg.in/juju/charm.v6-unstable"
	"gopkg.in/juju/charm.v6-unstable/resource"
	"gopkg.in/juju/charmrepo.v2-unstable/csclient/params"

	"gopkg.in/juju/charmstore.v5-unstable/internal/storetesting"
)

type ResourceSuite struct {
	commonSuite
}

var _ = gc.Suite(&ResourceSuite{})

func (s *ResourceSuite) SetUpSuite(c *gc.C) {
	s.enableES = false
	s.enableIdentity = true
	s.commonSuite.SetUpSuite(c)
}

func (s *ResourceSuite) TestPost(c *gc.C) {
	id := newResolvedURL("~charmers/precise/wordpress-0", -1)
	s.addPublicCharm(c, storetesting.NewCharm(&charm.Meta{
		Resources: map[string]resource.Meta{
			"someResource": {
				Name: "someResource",
				Type: resource.TypeFile,
				Path: "1.zip",
			},
		},
	}), id)
	content := "some content"
	hash := fmt.Sprintf("%x", sha512.Sum384([]byte(content)))
	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler:      s.srv,
		Method:       "POST",
		Body:         strings.NewReader(content),
		URL:          storeURL(fmt.Sprintf("%s/resource/someResource?hash=%s&filename=foo.zip", id.URL.Path(), hash)),
		ExpectStatus: http.StatusOK,
		ExpectBody: params.ResourceUploadResponse{
			// Note: revision 1 because addPublicCharm has already uploaded
			// revision 0.
			Revision: 1,
		},
		Do: s.bakeryDoAsUser(c, "charmers"),
	})

	// Check that the resource has really been uploaded.
	r, err := s.store.ResolveResource(id, "someResource", 1, "")
	c.Assert(err, gc.IsNil)

	blob, err := s.store.OpenResourceBlob(r)
	c.Assert(err, gc.IsNil)
	defer blob.Close()
	data, err := ioutil.ReadAll(blob)
	c.Assert(err, gc.IsNil)
	c.Assert(string(data), gc.Equals, content)
}

func (s *ResourceSuite) TestGet(c *gc.C) {
	id := newResolvedURL("~charmers/precise/wordpress-0", -1)
	meta := storetesting.MetaWithResources(nil, "someResource")
	s.store.AddCharmWithArchive(id, storetesting.NewCharm(meta))

	// Get with no revision should get a "not found" error because there are
	// no resources associated with the published charm.
	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler:      s.srv,
		Method:       "GET",
		URL:          storeURL(id.URL.Path() + "/resource/someResource"),
		ExpectStatus: http.StatusNotFound,
		ExpectBody: params.Error{
			Code:    params.ErrNotFound,
			Message: `cs:~charmers/precise/wordpress-0 has no "someResource" resource`,
		},
		Do: s.bakeryDoAsUser(c, "charmers"),
	})

	content := "some content"
	s.uploadResource(c, id, "someResource", content+"0")
	s.uploadResource(c, id, "someResource", content+"1")
	s.uploadResource(c, id, "someResource", content+"2")

	// Get with the revision should return the resource.
	resp := httptesting.DoRequest(c, httptesting.DoRequestParams{
		Handler: s.srv,
		Method:  "GET",
		URL:     storeURL(id.URL.Path() + "/resource/someResource/1"),
		Do:      s.bakeryDoAsUser(c, "charmers"),
	})
	c.Assert(resp.Body.String(), gc.Equals, content+"1")
	c.Assert(resp.Header().Get(params.ContentHashHeader), gc.Equals, hashOfString(content+"1"))
	c.Assert(resp.Code, gc.Equals, http.StatusOK)
	assertCacheControl(c, resp.Header(), false)

	// If we publish the resource, it should be available with no revision.
	err := s.store.Publish(id, map[string]int{"someResource": 2}, params.StableChannel)
	c.Assert(err, gc.IsNil)

	// Make it public so that we can check that the cache-control
	// headers change appropriately.
	err = s.store.SetPerms(&id.URL, "stable.read", params.Everyone)
	c.Assert(err, gc.IsNil)

	resp = httptesting.DoRequest(c, httptesting.DoRequestParams{
		Handler: s.srv,
		Method:  "GET",
		URL:     storeURL(id.URL.Path() + "/resource/someResource"),
	})
	c.Assert(resp.Body.String(), gc.Equals, content+"2")
	c.Assert(resp.Header().Get(params.ContentHashHeader), gc.Equals, hashOfString(content+"2"))
	c.Assert(resp.Code, gc.Equals, http.StatusOK)
	assertCacheControl(c, resp.Header(), true)
}

func (s *ResourceSuite) TestInvalidMethod(c *gc.C) {
	id := newResolvedURL("~charmers/precise/wordpress-0", -1)
	s.addPublicCharm(c, storetesting.NewCharm(nil), id)
	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler:      s.srv,
		Method:       "PUT",
		URL:          storeURL(id.URL.Path() + "/resource/someResource"),
		ExpectStatus: http.StatusMethodNotAllowed,
		ExpectBody: params.Error{
			Code:    params.ErrMethodNotAllowed,
			Message: `PUT not allowed`,
		},
		Do: s.bakeryDoAsUser(c, "charmers"),
	})
}

func (s *ResourceSuite) TestCannotUploadToBundle(c *gc.C) {
	id, _ := s.addPublicBundleFromRepo(c, "wordpress-simple", newResolvedURL("cs:~charmers/bundle/something-32", 32), true)

	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler:      s.srv,
		Method:       "POST",
		URL:          storeURL(id.URL.Path() + "/resource/someResource"),
		ExpectStatus: http.StatusForbidden,
		ExpectBody: params.Error{
			Code:    params.ErrForbidden,
			Message: `cannot upload a resource to a bundle`,
		},
		Do: s.bakeryDoAsUser(c, "charmers"),
	})
}

func (s *ResourceSuite) TestUploadInvalidResourceName(c *gc.C) {
	id := newResolvedURL("~charmers/precise/wordpress-0", -1)
	s.addPublicCharm(c, storetesting.NewCharm(nil), id)
	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler:      s.srv,
		Method:       "POST",
		URL:          storeURL(id.URL.Path() + "/resource/someResource/x"),
		ExpectStatus: http.StatusBadRequest,
		ExpectBody: params.Error{
			Code:    params.ErrBadRequest,
			Message: `invalid resource name`,
		},
		Do: s.bakeryDoAsUser(c, "charmers"),
	})
}

func (s *ResourceSuite) TestUploadNoHash(c *gc.C) {
	id := newResolvedURL("~charmers/precise/wordpress-0", -1)
	s.addPublicCharm(c, storetesting.NewCharm(nil), id)
	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler:      s.srv,
		Method:       "POST",
		URL:          storeURL(id.URL.Path() + "/resource/someResource"),
		ExpectStatus: http.StatusBadRequest,
		ExpectBody: params.Error{
			Code:    params.ErrBadRequest,
			Message: `hash parameter not specified`,
		},
		Do: s.bakeryDoAsUser(c, "charmers"),
	})
}

func (s *ResourceSuite) TestUploadNoContentLength(c *gc.C) {
	type exoticReader struct {
		io.ReadSeeker
	}

	id := newResolvedURL("~charmers/precise/wordpress-0", -1)
	s.addPublicCharm(c, storetesting.NewCharm(nil), id)

	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler:      s.srv,
		Method:       "POST",
		URL:          storeURL(id.URL.Path() + "/resource/someResource?hash=d0gf00d"),
		Body:         exoticReader{strings.NewReader("x")},
		ExpectStatus: http.StatusBadRequest,
		ExpectBody: params.Error{
			Code:    params.ErrBadRequest,
			Message: `Content-Length not specified`,
		},
		Do: s.bakeryDoAsUser(c, "charmers"),
	})
}

func (s *ResourceSuite) TestUploadResourceNotDeclaredInCharm(c *gc.C) {
	id := newResolvedURL("~charmers/precise/wordpress-0", -1)
	s.addPublicCharm(c, storetesting.NewCharm(nil), id)

	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler:      s.srv,
		Method:       "POST",
		URL:          storeURL(id.URL.Path() + "/resource/someResource?hash=d0gf00d"),
		ExpectStatus: http.StatusForbidden,
		ExpectBody: params.Error{
			Code:    params.ErrForbidden,
			Message: `resource "someResource" not found in charm metadata`,
		},
		Do: s.bakeryDoAsUser(c, "charmers"),
	})
}

func (s *ResourceSuite) TestUploadResourceFilenameExtensionMismatch(c *gc.C) {
	id := newResolvedURL("~charmers/precise/wordpress-0", -1)
	s.addPublicCharm(c, storetesting.NewCharm(&charm.Meta{
		Resources: map[string]resource.Meta{
			"someResource": {
				Name: "someResource",
				Type: resource.TypeFile,
				Path: "1.zip",
			},
		},
	}), id)

	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler:      s.srv,
		Method:       "POST",
		URL:          storeURL(id.URL.Path() + "/resource/someResource?hash=d0gf00d&filename=foo.dat"),
		ExpectStatus: http.StatusForbidden,
		ExpectBody: params.Error{
			Code:    params.ErrForbidden,
			Message: `filename extension mismatch (got ".dat" want ".zip")`,
		},
		Do: s.bakeryDoAsUser(c, "charmers"),
	})
}

func (s *ResourceSuite) TestUploadResourceFilenameWithNoExtension(c *gc.C) {
	id := newResolvedURL("~charmers/precise/wordpress-0", -1)
	s.addPublicCharm(c, storetesting.NewCharm(&charm.Meta{
		Resources: map[string]resource.Meta{
			"someResource": {
				Name: "someResource",
				Type: resource.TypeFile,
				Path: "1.zip",
			},
		},
	}), id)

	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler:      s.srv,
		Method:       "POST",
		URL:          storeURL(id.URL.Path() + "/resource/someResource?hash=d0gf00d&filename=foo"),
		ExpectStatus: http.StatusForbidden,
		ExpectBody: params.Error{
			Code:    params.ErrForbidden,
			Message: `filename extension mismatch (got "" want ".zip")`,
		},
		Do: s.bakeryDoAsUser(c, "charmers"),
	})
}

func (s *ResourceSuite) TestUploadResourcePathWithNoExtension(c *gc.C) {
	// If the resource path has no extension, we don't check the
	// file extension.
	id := newResolvedURL("~charmers/precise/wordpress-0", -1)
	s.addPublicCharm(c, storetesting.NewCharm(&charm.Meta{
		Resources: map[string]resource.Meta{
			"someResource": {
				Name: "someResource",
				Type: resource.TypeFile,
				Path: "something",
			},
		},
	}), id)
	content := "x"
	hash := fmt.Sprintf("%x", sha512.Sum384([]byte(content)))
	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler: s.srv,
		Method:  "POST",
		URL:     storeURL(id.URL.Path() + "/resource/someResource?hash=" + hash + "&filename=foo.zip"),
		Body:    strings.NewReader(content),
		ExpectBody: params.ResourceUploadResponse{
			// Note: revision 1 because addPublicCharm has already uploaded
			// revision 0.
			Revision: 1,
		},
		Do: s.bakeryDoAsUser(c, "charmers"),
	})
}

func (s *ResourceSuite) TestDownloadBadResourceRevision(c *gc.C) {
	id := newResolvedURL("~charmers/precise/wordpress-0", -1)
	s.addPublicCharm(c, storetesting.NewCharm(&charm.Meta{
		Resources: map[string]resource.Meta{
			"someResource": {
				Name: "someResource",
				Type: resource.TypeFile,
				Path: "1.zip",
			},
		},
	}), id)
	content := "some content"
	s.uploadResource(c, id, "someResource", content)

	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler:      s.srv,
		Method:       "GET",
		URL:          storeURL(id.URL.Path() + "/resource/someResource/x"),
		ExpectStatus: http.StatusNotFound,
		ExpectBody: params.Error{
			Code:    params.ErrNotFound,
			Message: `not found: malformed revision number`,
		},
	})
}

func (s *ResourceSuite) TestDownloadResourceNotFound(c *gc.C) {
	id := newResolvedURL("~charmers/precise/wordpress-0", -1)
	s.addPublicCharm(c, storetesting.NewCharm(&charm.Meta{
		Resources: map[string]resource.Meta{
			"someResource": {
				Name: "someResource",
				Type: resource.TypeFile,
				Path: "1.zip",
			},
		},
	}), id)
	content := "some content"
	s.uploadResource(c, id, "someResource", content)

	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler:      s.srv,
		Method:       "GET",
		URL:          storeURL(id.URL.Path() + "/resource/otherResource/0"),
		ExpectStatus: http.StatusNotFound,
		ExpectBody: params.Error{
			Code:    params.ErrNotFound,
			Message: `cs:~charmers/precise/wordpress-0 has no "otherResource/0" resource`,
		},
	})
}

func (s *ResourceSuite) TestDownloadPrivateCharmResource(c *gc.C) {
	id := newResolvedURL("~charmers/precise/wordpress-0", -1)
	err := s.store.AddCharmWithArchive(id, storetesting.NewCharm(&charm.Meta{
		Resources: map[string]resource.Meta{
			"someResource": {
				Name: "someResource",
				Type: resource.TypeFile,
				Path: "1.zip",
			},
		},
	}))
	c.Assert(err, gc.IsNil)
	content := "some content"
	s.uploadResource(c, id, "someResource", content)

	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler:      s.srv,
		Method:       "GET",
		URL:          storeURL(id.URL.Path() + "/resource/someResource/0"),
		ExpectStatus: http.StatusUnauthorized,
		ExpectBody: params.Error{
			Code:    params.ErrUnauthorized,
			Message: `unauthorized: access denied for user "bob"`,
		},
		Do: s.bakeryDoAsUser(c, "bob"),
	})

	resp := httptesting.DoRequest(c, httptesting.DoRequestParams{
		Handler: s.srv,
		Method:  "GET",
		URL:     storeURL(id.URL.Path() + "/resource/someResource/0"),
		Do:      s.bakeryDoAsUser(c, "charmers"),
	})
	c.Assert(resp.Body.String(), gc.Equals, content)
	c.Assert(resp.Header().Get(params.ContentHashHeader), gc.Equals, hashOfString(content))
	c.Assert(resp.Code, gc.Equals, http.StatusOK)
	assertCacheControl(c, resp.Header(), false)
}

func (s *ResourceSuite) TestEmptyListResource(c *gc.C) {
	id := newResolvedURL("~charmers/precise/wordpress-0", 0)
	s.addPublicCharmFromRepo(c, "wordpress", id)

	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler:      s.srv,
		URL:          storeURL(id.URL.Path() + "/meta/resources"),
		ExpectStatus: http.StatusOK,
		ExpectBody:   []params.Resource{},
	})
}

func (s *ResourceSuite) TestListResource(c *gc.C) {
	id := newResolvedURL("~charmers/precise/wordpress-0", -1)
	s.addPublicCharm(c, storetesting.NewCharm(storetesting.MetaWithResources(nil, "resource1", "resource2")), id)
	s.uploadResource(c, id, "resource1", "resource1 content")
	s.uploadResource(c, id, "resource2", "resource2 content")

	err := s.store.Publish(id, map[string]int{
		"resource1": 0,
		"resource2": 0,
	}, params.StableChannel)
	c.Assert(err, gc.IsNil)

	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler:      s.srv,
		URL:          storeURL(id.URL.Path() + "/meta/resources"),
		ExpectStatus: http.StatusOK,
		ExpectBody: []params.Resource{{
			Name:        "resource1",
			Type:        "file",
			Path:        "resource1-file",
			Description: "resource1 description",
			Revision:    0,
			Fingerprint: rawHash(hashOfString("resource1 content")),
			Size:        int64(len("resource1 content")),
		}, {
			Name:        "resource2",
			Type:        "file",
			Path:        "resource2-file",
			Description: "resource2 description",
			Revision:    0,
			Fingerprint: rawHash(hashOfString("resource2 content")),
			Size:        int64(len("resource2 content")),
		}},
	})
}

func (s *ResourceSuite) TestListResourceWithBundle(c *gc.C) {
	id := newResolvedURL("cs:~charmers/bundle/bundlelovin-10", 10)
	s.addPublicBundleFromRepo(c, "wordpress-simple", id, true)
	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler:      s.srv,
		URL:          storeURL(id.URL.Path() + "/meta/resources"),
		ExpectStatus: http.StatusNotFound,
		ExpectBody: params.Error{
			Code:    params.ErrMetadataNotFound,
			Message: string(params.ErrMetadataNotFound),
		},
	})
}
