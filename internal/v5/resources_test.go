// Copyright 2016 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package v5_test

import (
	"crypto/sha512"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"

	jc "github.com/juju/testing/checkers"
	"github.com/juju/testing/httptesting"
	gc "gopkg.in/check.v1"
	"gopkg.in/errgo.v1"
	"gopkg.in/juju/charm.v6"
	"gopkg.in/juju/charm.v6/resource"
	"gopkg.in/juju/charmrepo.v3/csclient/params"

	"gopkg.in/juju/charmstore.v5/internal/blobstore"
	"gopkg.in/juju/charmstore.v5/internal/storetesting"
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
		Do: s.bakeryDoAsUser("charmers"),
	})

	// Check that the resource has really been uploaded.
	r, err := s.store.ResolveResource(id, "someResource", 1, "")
	c.Assert(err, gc.Equals, nil)

	blob, err := s.store.OpenResourceBlob(r)
	c.Assert(err, gc.Equals, nil)
	defer blob.Close()
	data, err := ioutil.ReadAll(blob)
	c.Assert(err, gc.Equals, nil)
	c.Assert(string(data), gc.Equals, content)
}

func (s *ResourceSuite) TestMultipartPost(c *gc.C) {
	// Create the upload.
	resp := httptesting.DoRequest(c, httptesting.DoRequestParams{
		Handler: s.srv,
		Do:      bakeryDo(s.idmServer.Client("bob")),
		Method:  "POST",
		URL:     storeURL("upload"),
	})
	var uploadIdResp params.UploadInfoResponse
	err := json.Unmarshal(resp.Body.Bytes(), &uploadIdResp)
	c.Assert(err, gc.Equals, nil)
	uploadId := uploadIdResp.UploadId

	// Upload each part in turn.
	contents := []string{
		"12345689 123456789 ",
		"abcdefghijklmnopqrstuvwxyz",
	}
	allContents := strings.Join(contents, "")

	pos := int64(0)
	for i, content := range contents {
		httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
			Handler: s.srv,
			Method:  "PUT",
			Do:      bakeryDo(s.idmServer.Client("bob")),
			URL:     storeURL(fmt.Sprintf("upload/%s/%d?hash=%s&offset=%d", uploadId, i, hashOfString(content), pos)),
			Body:    strings.NewReader(content),
		})
		pos += int64(len(content))
	}

	// Finalize the upload.
	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler: s.srv,
		Method:  "PUT",
		Do:      bakeryDo(s.idmServer.Client("bob")),
		URL:     storeURL("upload/" + uploadId),
		JSONBody: params.Parts{
			Parts: []params.Part{{
				Hash: hashOfString(contents[0]),
			}, {
				Hash: hashOfString(contents[1]),
			}},
		},
		ExpectBody: &params.FinishUploadResponse{
			Hash: hashOfString(allContents),
		},
	})

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

	// Create the resource.
	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler: s.srv,
		Method:  "POST",
		URL:     storeURL(fmt.Sprintf("%s/resource/someResource?upload-id=%s", id.URL.Path(), uploadId)),
		ExpectBody: params.ResourceUploadResponse{
			// Note: revision 1 because addPublicCharm has already uploaded
			// revision 0.
			Revision: 1,
		},
		Do: s.bakeryDoAsUser("charmers"),
	})

	// Check that the resource has really been uploaded.
	r, err := s.store.ResolveResource(id, "someResource", 1, "")
	c.Assert(err, gc.Equals, nil)

	blob, err := s.store.OpenResourceBlob(r)
	c.Assert(err, gc.Equals, nil)
	defer blob.Close()
	data, err := ioutil.ReadAll(blob)
	c.Assert(err, gc.Equals, nil)
	c.Assert(string(data), gc.Equals, allContents)

	// Check that the upload info has been removed.
	_, err = s.store.BlobStore.UploadInfo(uploadId)
	c.Assert(errgo.Cause(err), gc.Equals, blobstore.ErrNotFound, gc.Commentf("error: %v", err))
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
		Do: s.bakeryDoAsUser("charmers"),
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
		Do:      s.bakeryDoAsUser("charmers"),
	})
	c.Assert(resp.Body.String(), gc.Equals, content+"1")
	c.Assert(resp.Header().Get(params.ContentHashHeader), gc.Equals, hashOfString(content+"1"))
	c.Assert(resp.Code, gc.Equals, http.StatusOK)
	assertCacheControl(c, resp.Header(), false)

	// If we publish the resource, it should be available with no revision.
	err := s.store.Publish(id, map[string]int{"someResource": 2}, params.StableChannel)
	c.Assert(err, gc.Equals, nil)

	// Make it public so that we can check that the cache-control
	// headers change appropriately.
	err = s.store.SetPerms(&id.URL, "stable.read", params.Everyone)
	c.Assert(err, gc.Equals, nil)

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
		Do: s.bakeryDoAsUser("charmers"),
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
		Do: s.bakeryDoAsUser("charmers"),
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
		Do: s.bakeryDoAsUser("charmers"),
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
		Do: s.bakeryDoAsUser("charmers"),
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
		Do: s.bakeryDoAsUser("charmers"),
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
		Do: s.bakeryDoAsUser("charmers"),
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
		Do: s.bakeryDoAsUser("charmers"),
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
		Do: s.bakeryDoAsUser("charmers"),
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
		Do: s.bakeryDoAsUser("charmers"),
	})
}

func (s *ResourceSuite) TestUploadResourceDockerImage(c *gc.C) {
	id := newResolvedURL("~charmers/kubecharm-0", -1)
	err := s.store.AddCharmWithArchive(id, storetesting.NewCharm(&charm.Meta{
		Series: []string{"kubernetes"},
		Resources: map[string]resource.Meta{
			"someResource": {
				Name: "someResource",
				Type: resource.TypeContainerImage,
			},
		},
	}))
	c.Assert(err, gc.Equals, nil)
	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler: s.srv,
		Method:  "POST",
		URL:     storeURL(id.URL.Path() + "/resource/someResource"),
		JSONBody: params.DockerResourceUploadRequest{
			Digest: "sha256:d1d44afba88cabf44cccd8d9fde2daacba31e09e9b7e46526ba9c1e3b41c0a3b",
		},
		ExpectBody: params.ResourceUploadResponse{
			Revision: 0,
		},
		Do: s.bakeryDoAsUser("charmers"),
	})
}

type DockerResourceGetResponse struct {
	ImageName string
	Username  string
	Password  string
}

func (s *ResourceSuite) TestGetResourceDockerImage(c *gc.C) {
	id := newResolvedURL("~charmers/kubecharm-0", -1)
	err := s.store.AddCharmWithArchive(id, storetesting.NewCharm(&charm.Meta{
		Series: []string{"kubernetes"},
		Resources: map[string]resource.Meta{
			"someResource": {
				Name: "someResource",
				Type: resource.TypeContainerImage,
			},
		},
	}))
	c.Assert(err, gc.Equals, nil)
	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler: s.srv,
		Method:  "POST",
		URL:     storeURL(id.URL.Path() + "/resource/someResource"),
		JSONBody: params.DockerResourceUploadRequest{
			Digest: "sha256:d1d44afba88cabf44cccd8d9fde2daacba31e09e9b7e46526ba9c1e3b41c0a3b",
		},
		ExpectBody: params.ResourceUploadResponse{
			Revision: 0,
		},
		Do: s.bakeryDoAsUser("charmers"),
	})
	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler: s.srv,
		Method:  "GET",
		URL:     storeURL(id.URL.Path() + "/resource/someResource/0"),
		ExpectBody: httptesting.BodyAsserter(func(c *gc.C, m json.RawMessage) {
			var resp params.DockerInfoResponse
			err := json.Unmarshal(m, &resp)
			c.Assert(err, gc.Equals, nil)
			password := resp.Password
			resp.Password = ""
			c.Assert(resp, jc.DeepEquals, params.DockerInfoResponse{
				ImageName: "dockerregistry.example.com/charmers/kubecharm/someResource@sha256:d1d44afba88cabf44cccd8d9fde2daacba31e09e9b7e46526ba9c1e3b41c0a3b",
				Username:  "docker-registry",
			})
			c.Assert(password, gc.Not(gc.Equals), "")
		}),
		Do: s.bakeryDoAsUser("charmers"),
	})

}

func (s *ResourceSuite) TestGetResourceDockerImageWithExplicitImageName(c *gc.C) {
	id := newResolvedURL("~charmers/kubecharm-0", -1)
	err := s.store.AddCharmWithArchive(id, storetesting.NewCharm(&charm.Meta{
		Series: []string{"kubernetes"},
		Resources: map[string]resource.Meta{
			"someResource": {
				Name: "someResource",
				Type: resource.TypeContainerImage,
			},
		},
	}))
	c.Assert(err, gc.Equals, nil)
	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler: s.srv,
		Method:  "POST",
		URL:     storeURL(id.URL.Path() + "/resource/someResource"),
		JSONBody: params.DockerResourceUploadRequest{
			ImageName: "0.1.2.3/someimage",
			Digest:    "sha256:d1d44afba88cabf44cccd8d9fde2daacba31e09e9b7e46526ba9c1e3b41c0a3b",
		},
		ExpectBody: params.ResourceUploadResponse{
			Revision: 0,
		},
		Do: s.bakeryDoAsUser("charmers"),
	})
	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler: s.srv,
		Method:  "GET",
		URL:     storeURL(id.URL.Path() + "/resource/someResource/0"),
		ExpectBody: params.DockerInfoResponse{
			ImageName: "0.1.2.3/someimage@sha256:d1d44afba88cabf44cccd8d9fde2daacba31e09e9b7e46526ba9c1e3b41c0a3b",
		},
		Do: s.bakeryDoAsUser("charmers"),
	})

}

func (s *ResourceSuite) TestGetResourceDockerImageUploadInfo(c *gc.C) {
	id := newResolvedURL("~charmers/kubecharm-0", -1)
	err := s.store.AddCharmWithArchive(id, storetesting.NewCharm(&charm.Meta{
		Series: []string{"kubernetes"},
		Resources: map[string]resource.Meta{
			"someResource": {
				Name: "someResource",
				Type: resource.TypeContainerImage,
			},
		},
	}))
	c.Assert(err, gc.Equals, nil)
	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler: s.srv,
		Method:  "GET",
		URL:     storeURL(id.URL.Path() + "/docker-resource-upload-info?resource-name=someResource"),
		ExpectBody: httptesting.BodyAsserter(func(c *gc.C, m json.RawMessage) {
			var resp params.DockerInfoResponse
			err := json.Unmarshal(m, &resp)
			c.Assert(err, gc.Equals, nil)
			password := resp.Password
			resp.Password = ""
			c.Assert(resp, jc.DeepEquals, params.DockerInfoResponse{
				ImageName: "dockerregistry.example.com/charmers/kubecharm/someResource",
				Username:  "docker-uploader",
			})
			c.Assert(password, gc.Not(gc.Equals), "")
		}),
		Do: s.bakeryDoAsUser("charmers"),
	})
}

func (s *ResourceSuite) TestGetResourceDockerImageUploadInfoNoResourceName(c *gc.C) {
	id := newResolvedURL("~charmers/kubecharm-0", -1)
	err := s.store.AddCharmWithArchive(id, storetesting.NewCharm(&charm.Meta{
		Series: []string{"kubernetes"},
		Resources: map[string]resource.Meta{
			"someResource": {
				Name: "someResource",
				Type: resource.TypeContainerImage,
			},
		},
	}))
	c.Assert(err, gc.Equals, nil)
	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler:      s.srv,
		Method:       "GET",
		URL:          storeURL(id.URL.Path() + "/docker-resource-upload-info"),
		ExpectStatus: http.StatusBadRequest,
		ExpectBody: params.Error{
			Code:    params.ErrBadRequest,
			Message: `must specify resource-name parameter`,
		},
		Do: s.bakeryDoAsUser("charmers"),
	})
}

func (s *ResourceSuite) TestGetResourceDockerImageUploadInfoForNonExistentCharm(c *gc.C) {
	id := newResolvedURL("~charmers/kubecharm-0", -1)
	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler:      s.srv,
		Method:       "GET",
		URL:          storeURL(id.URL.Path() + "/docker-resource-upload-info?resource-name=someResource"),
		ExpectStatus: http.StatusNotFound,
		ExpectBody: params.Error{
			Code:    params.ErrNotFound,
			Message: `no matching charm or bundle for cs:~charmers/kubecharm-0`,
		},
	})
}

func (s *ResourceSuite) TestGetResourceDockerImageUploadInfoForNonExistentResource(c *gc.C) {
	id := newResolvedURL("~charmers/kubecharm-0", -1)
	err := s.store.AddCharmWithArchive(id, storetesting.NewCharm(&charm.Meta{
		Series: []string{"kubernetes"},
		Resources: map[string]resource.Meta{
			"someResource": {
				Name: "someResource",
				Type: resource.TypeContainerImage,
			},
		},
	}))
	c.Assert(err, gc.Equals, nil)
	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler:      s.srv,
		Method:       "GET",
		URL:          storeURL(id.URL.Path() + "/docker-resource-upload-info?resource-name=otherResource"),
		ExpectStatus: http.StatusForbidden,
		ExpectBody: params.Error{
			Code:    params.ErrForbidden,
			Message: `"cs:~charmers/kubecharm-0" has no resource named "otherResource"`,
		},
		Do: s.bakeryDoAsUser("charmers"),
	})
}

func (s *ResourceSuite) TestGetResourceDockerImageUploadInfoForBundle(c *gc.C) {
	id := newResolvedURL("~charmers/bundle/something-0", -1)
	s.addPublicBundle(c, relationTestingBundle([]string{
		"cs:utopic/wordpress-42",
	}), id, true)
	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler:      s.srv,
		Method:       "GET",
		URL:          storeURL(id.URL.Path() + "/docker-resource-upload-info?resource-name=otherResource"),
		ExpectStatus: http.StatusForbidden,
		ExpectBody: params.Error{
			Code:    params.ErrForbidden,
			Message: `"cs:~charmers/bundle/something-0" does not support docker resource upload`,
		},
		Do: s.bakeryDoAsUser("charmers"),
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
			Message: `malformed revision number`,
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
	c.Assert(err, gc.Equals, nil)
	content := "some content"
	s.uploadResource(c, id, "someResource", content)

	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler:      s.srv,
		Method:       "GET",
		URL:          storeURL(id.URL.Path() + "/resource/someResource/0"),
		ExpectStatus: http.StatusUnauthorized,
		ExpectBody: params.Error{
			Code:    params.ErrUnauthorized,
			Message: `access denied for user "bob"`,
		},
		Do: s.bakeryDoAsUser("bob"),
	})

	resp := httptesting.DoRequest(c, httptesting.DoRequestParams{
		Handler: s.srv,
		Method:  "GET",
		URL:     storeURL(id.URL.Path() + "/resource/someResource/0"),
		Do:      s.bakeryDoAsUser("charmers"),
	})
	c.Assert(resp.Body.String(), gc.Equals, content)
	c.Assert(resp.Header().Get(params.ContentHashHeader), gc.Equals, hashOfString(content))
	c.Assert(resp.Code, gc.Equals, http.StatusOK)
	assertCacheControl(c, resp.Header(), false)
}

func (s *ResourceSuite) TestMetaResourcesWithNoResources(c *gc.C) {
	id := newResolvedURL("~charmers/precise/wordpress-0", 0)
	s.addPublicCharmFromRepo(c, "wordpress", id)

	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler:      s.srv,
		URL:          storeURL(id.URL.Path() + "/meta/resources"),
		ExpectStatus: http.StatusOK,
		ExpectBody:   []params.Resource{},
	})
}

func (s *ResourceSuite) TestMetaResources(c *gc.C) {
	id := newResolvedURL("~charmers/precise/wordpress-0", -1)
	s.addPublicCharm(c, storetesting.NewCharm(storetesting.MetaWithResources(nil, "resource1", "resource2")), id)
	s.uploadResource(c, id, "resource1", "resource1 content")
	s.uploadResource(c, id, "resource2", "resource2 content")

	err := s.store.Publish(id, map[string]int{
		"resource1": 0,
		"resource2": 0,
	}, params.StableChannel)
	c.Assert(err, gc.Equals, nil)

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

func (s *ResourceSuite) TestMetaResourcesWithBundle(c *gc.C) {
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

func (s *ResourceSuite) TestMetaResourcesSingleBadResourceId(c *gc.C) {
	id := newResolvedURL("~charmers/precise/wordpress-0", -1)
	s.addPublicCharm(c, storetesting.NewCharm(nil), id)
	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler:      s.srv,
		URL:          storeURL(id.URL.Path() + "/meta/resources/foo/bar"),
		ExpectStatus: http.StatusNotFound,
		ExpectBody: params.Error{
			Code:    params.ErrMetadataNotFound,
			Message: string(params.ErrMetadataNotFound),
		},
	})
}

func (s *ResourceSuite) TestMetaResourcesSingleResourceWithNegativeRevisionNumber(c *gc.C) {
	id := newResolvedURL("~charmers/precise/wordpress-0", -1)
	s.addPublicCharm(c, storetesting.NewCharm(nil), id)
	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler:      s.srv,
		URL:          storeURL(id.URL.Path() + "/meta/resources/foo/-2"),
		ExpectStatus: http.StatusNotFound,
		ExpectBody: params.Error{
			Code:    params.ErrMetadataNotFound,
			Message: string(params.ErrMetadataNotFound),
		},
	})
}

func (s *ResourceSuite) TestMetaResourcesSingleWithBundle(c *gc.C) {
	id, _ := s.addPublicBundleFromRepo(c, "wordpress-simple", newResolvedURL("cs:~charmers/bundle/something-32", 32), true)
	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler:      s.srv,
		URL:          storeURL(id.URL.Path() + "/meta/resources/foo"),
		ExpectStatus: http.StatusNotFound,
		ExpectBody: params.Error{
			Code:    params.ErrMetadataNotFound,
			Message: string(params.ErrMetadataNotFound),
		},
	})
}

func (s *ResourceSuite) TestMetaResourcesSingleResourceNotInCharm(c *gc.C) {
	id := newResolvedURL("~charmers/precise/wordpress-0", -1)
	s.addPublicCharm(c, storetesting.NewCharm(nil), id)
	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler:      s.srv,
		URL:          storeURL(id.URL.Path() + "/meta/resources/foo"),
		ExpectStatus: http.StatusNotFound,
		ExpectBody: params.Error{
			Code:    params.ErrMetadataNotFound,
			Message: string(params.ErrMetadataNotFound),
		},
	})
}

func (s *ResourceSuite) TestMetaResourcesSingleResourceNotUploaded(c *gc.C) {
	id := newResolvedURL("~charmers/precise/wordpress-0", -1)
	meta := storetesting.MetaWithResources(nil, "someResource")
	err := s.store.AddCharmWithArchive(id, storetesting.NewCharm(meta))
	c.Assert(err, gc.Equals, nil)

	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler: s.srv,
		URL:     storeURL(id.URL.Path() + "/meta/resources/someResource?channel=unpublished"),
		ExpectBody: params.Resource{
			Name:        "someResource",
			Type:        "file",
			Path:        "someResource-file",
			Description: "someResource description",
			Revision:    -1,
		},
		Do: s.bakeryDoAsUser("charmers"),
	})
}

func (s *ResourceSuite) TestMetaResourcesSingleResourceWithRevision(c *gc.C) {
	id := newResolvedURL("~charmers/precise/wordpress-0", -1)
	meta := storetesting.MetaWithResources(nil, "someResource")
	s.addPublicCharm(c, storetesting.NewCharm(meta), id)

	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler: s.srv,
		URL:     storeURL(id.URL.Path() + "/meta/resources/someResource/0"),
		ExpectBody: params.Resource{
			Name:        "someResource",
			Type:        "file",
			Path:        "someResource-file",
			Description: "someResource description",
			Revision:    0,
			Fingerprint: rawHash(hashOfString("someResource content")),
			Size:        int64(len("someResource content")),
		},
		Do: s.bakeryDoAsUser("charmers"),
	})
}

func (s *ResourceSuite) TestMetaResourcesSingleResourceWithRevisionNotFound(c *gc.C) {
	id := newResolvedURL("~charmers/precise/wordpress-0", -1)
	meta := storetesting.MetaWithResources(nil, "someResource")
	s.addPublicCharm(c, storetesting.NewCharm(meta), id)

	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler:      s.srv,
		URL:          storeURL(id.URL.Path() + "/meta/resources/someResource/1"),
		ExpectStatus: http.StatusNotFound,
		ExpectBody: params.Error{
			Code:    params.ErrMetadataNotFound,
			Message: string(params.ErrMetadataNotFound),
		},
		Do: s.bakeryDoAsUser("charmers"),
	})
}

func (s *ResourceSuite) TestMetaResourcesSingleResourceWithRevisionNotInCharm(c *gc.C) {
	id := newResolvedURL("~charmers/precise/wordpress-0", -1)
	meta := storetesting.MetaWithResources(nil, "someResource")
	s.addPublicCharm(c, storetesting.NewCharm(meta), id)

	s.uploadResource(c, id, "someResource", "a new version")

	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler: s.srv,
		URL:     storeURL(id.URL.Path() + "/meta/resources/someResource/1"),
		ExpectBody: params.Resource{
			Name:        "someResource",
			Type:        "file",
			Path:        "someResource-file",
			Description: "someResource description",
			Revision:    1,
			Fingerprint: rawHash(hashOfString("a new version")),
			Size:        int64(len("a new version")),
		},
		Do: s.bakeryDoAsUser("charmers"),
	})
}

func (s *ResourceSuite) TestMetaResourcesSingleResourceWithNameNotInCharm(c *gc.C) {
	id0 := newResolvedURL("~charmers/precise/wordpress-0", -1)
	meta := storetesting.MetaWithResources(nil, "someResource")
	s.addPublicCharm(c, storetesting.NewCharm(meta), id0)

	id1 := newResolvedURL("~charmers/precise/wordpress-1", -1)
	meta = storetesting.MetaWithResources(nil, "otherResource")
	s.addPublicCharm(c, storetesting.NewCharm(meta), id1)

	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler:      s.srv,
		URL:          storeURL(id0.URL.Path() + "/meta/resources/otherResource/0"),
		ExpectStatus: http.StatusNotFound,
		ExpectBody: params.Error{
			Code:    params.ErrMetadataNotFound,
			Message: string(params.ErrMetadataNotFound),
		},
		Do: s.bakeryDoAsUser("charmers"),
	})
}

func (s *ResourceSuite) TestMetaResourcesDockerResource(c *gc.C) {
	id := newResolvedURL("~charmers/caas-0", -1)
	meta := storetesting.MetaWithDockerResources(nil, "resource1", "resource2")
	meta = storetesting.MetaWithSupportedSeries(meta, "kubernetes")
	err := s.store.AddCharmWithArchive(id, storetesting.NewCharm(meta))
	c.Assert(err, gc.Equals, nil)
	s.addDockerResource(c, id, "resource1", "resource1 content")
	s.addDockerResource(c, id, "resource2", "resource2 content")

	err = s.store.Publish(id, map[string]int{
		"resource1": 0,
		"resource2": 0,
	}, params.StableChannel)
	c.Assert(err, gc.Equals, nil)
	err = s.store.SetPerms(&id.URL, "stable.read", params.Everyone)
	c.Assert(err, gc.Equals, nil)

	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler:      s.srv,
		URL:          storeURL(id.URL.Path() + "/meta/resources"),
		ExpectStatus: http.StatusOK,
		ExpectBody: []params.Resource{{
			Name:        "resource1",
			Type:        "oci-image",
			Description: "resource1 description",
			Revision:    0,
		}, {
			Name:        "resource2",
			Type:        "oci-image",
			Description: "resource2 description",
			Revision:    0,
		}},
	})
}
