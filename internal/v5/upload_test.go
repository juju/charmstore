// Copyright 2017 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package v5_test

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	jc "github.com/juju/testing/checkers"
	"github.com/juju/testing/httptesting"
	gc "gopkg.in/check.v1"
	"gopkg.in/juju/charmrepo.v2-unstable/csclient/params"
)

func (s *APISuite) TestPostUploadFailsWithNoMacaroon(c *gc.C) {
	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler:      s.noMacaroonSrv,
		URL:          storeURL("upload"),
		Do:           bakeryDo(nil),
		ExpectStatus: http.StatusUnauthorized,
		ExpectBody: params.Error{
			Code:    params.ErrUnauthorized,
			Message: "authentication failed: missing HTTP auth header",
		},
	})
}

func (s *APISuite) TestPostUpload(c *gc.C) {
	now := time.Now()
	resp := httptesting.DoRequest(c, httptesting.DoRequestParams{
		Handler: s.srv,
		Method:  "POST",
		Do:      bakeryDo(s.idmServer.Client("bob")),
		URL:     storeURL("upload?expires=2m"),
	})
	var uploadResp params.NewUploadResponse
	err := json.Unmarshal(resp.Body.Bytes(), &uploadResp)
	c.Assert(err, gc.Equals, nil)

	uploadId := uploadResp.UploadId
	c.Assert(uploadId, gc.Not(gc.Equals), "")

	expires := uploadResp.Expires
	if got, want := expires, now.Add(2*time.Minute).Truncate(time.Millisecond); got.Before(want) {
		c.Errorf("expires too early, got %v, want %v", got, want)
	}
	if got, want := expires, now.Add(2*time.Minute+5*time.Second); got.After(want) {
		c.Errorf("expires too late, got %v, want %v", got, want)
	}
	c.Assert(uploadResp, jc.DeepEquals, params.NewUploadResponse{
		UploadId:    uploadId,
		Expires:     expires,
		MinPartSize: s.store.BlobStore.MinPartSize,
		MaxPartSize: s.store.BlobStore.MaxPartSize,
		MaxParts:    s.store.BlobStore.MaxParts,
	})

	info, err := s.store.BlobStore.UploadInfo(uploadId)
	c.Assert(err, gc.Equals, nil)
	c.Assert(len(info.Parts), gc.Equals, 0)
	c.Assert(info.Hash, gc.Equals, "")
	c.Assert(info.Expires.UTC(), gc.Equals, expires.UTC())
}

func (s *APISuite) TestPostUploadMaxExpiry(c *gc.C) {
	now := time.Now()
	resp := httptesting.DoRequest(c, httptesting.DoRequestParams{
		Handler: s.srv,
		Method:  "POST",
		Do:      bakeryDo(s.idmServer.Client("bob")),
		URL:     storeURL("upload?expires=27h"),
	})
	var uploadResp params.NewUploadResponse
	err := json.Unmarshal(resp.Body.Bytes(), &uploadResp)
	c.Assert(err, gc.Equals, nil)
	info, err := s.store.BlobStore.UploadInfo(uploadResp.UploadId)
	c.Assert(err, gc.Equals, nil)
	c.Assert(len(info.Parts), gc.Equals, 0)
	c.Assert(info.Hash, gc.Equals, "")
	if want := now.Add(24 * time.Hour).Truncate(time.Millisecond); info.Expires.Before(want) {
		c.Errorf("expires too early, got %v, want %v", info.Expires, want)
	}
	if want := now.Add(24*time.Hour + 5*time.Second); info.Expires.After(want) {
		c.Errorf("expires too late, got %v, want %v", info.Expires, want)
	}
}

func (s *APISuite) TestPostUploadInvalidExpiry(c *gc.C) {
	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler:      s.srv,
		Method:       "POST",
		Do:           bakeryDo(s.idmServer.Client("bob")),
		URL:          storeURL("upload?expires=somethingwrong"),
		ExpectStatus: http.StatusBadRequest,
		ExpectBody: params.Error{
			Message: `cannot parse expires "somethingwrong"`,
			Code:    params.ErrBadRequest,
		},
	})
}

func (s *APISuite) TestPostUploadNoExpiry(c *gc.C) {
	now := time.Now()
	resp := httptesting.DoRequest(c, httptesting.DoRequestParams{
		Handler: s.srv,
		Do:      bakeryDo(s.idmServer.Client("bob")),
		Method:  "POST",
		URL:     storeURL("upload"),
	})
	var uploadResp params.NewUploadResponse
	err := json.Unmarshal(resp.Body.Bytes(), &uploadResp)
	c.Assert(err, gc.Equals, nil)
	info, err := s.store.BlobStore.UploadInfo(uploadResp.UploadId)
	c.Assert(err, gc.Equals, nil)
	c.Assert(len(info.Parts), gc.Equals, 0)
	c.Assert(info.Hash, gc.Equals, "")
	if want := now.Add(24 * time.Hour).Truncate(time.Millisecond); info.Expires.Before(want) {
		c.Errorf("expires too early, got %v, want %v", info.Expires, want)
	}
	if want := now.Add(24*time.Hour + 5*time.Second); info.Expires.After(want) {
		c.Errorf("expires too late, got %v, want %v", info.Expires, want)
	}
}

func (s *APISuite) TestPutUploadFailsWithNoMacaroon(c *gc.C) {
	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler:      s.noMacaroonSrv,
		Method:       "PUT",
		URL:          storeURL("upload/someid"),
		JSONBody:     params.Parts{},
		ExpectStatus: http.StatusUnauthorized,
		ExpectBody: params.Error{
			Code:    params.ErrUnauthorized,
			Message: "authentication failed: missing HTTP auth header",
		},
	})
}

func (s *APISuite) TestPutUploadNoParts(c *gc.C) {
	resp := httptesting.DoRequest(c, httptesting.DoRequestParams{
		Handler: s.srv,
		Method:  "POST",
		Do:      bakeryDo(s.idmServer.Client("bob")),
		URL:     storeURL("upload"),
	})
	var uploadResp params.NewUploadResponse
	err := json.Unmarshal(resp.Body.Bytes(), &uploadResp)
	c.Assert(err, gc.Equals, nil)
	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler:  s.srv,
		Method:   "PUT",
		Do:       bakeryDo(s.idmServer.Client("bob")),
		URL:      storeURL("upload/" + uploadResp.UploadId),
		JSONBody: params.Parts{},
		ExpectBody: &params.FinishUploadResponse{
			Hash: hashOfString(""),
		},
	})
}

func (s *APISuite) TestPutUploadInvalidParts(c *gc.C) {
	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler:      s.srv,
		Method:       "PUT",
		Do:           bakeryDo(s.idmServer.Client("bob")),
		URL:          storeURL("upload/someid"),
		Body:         strings.NewReader("somethingwrong"),
		ExpectStatus: http.StatusBadRequest,
		ExpectBody: params.Error{
			Message: `cannot parse body: invalid character 's' looking for beginning of value`,
			Code:    params.ErrBadRequest,
		},
	})
}

func (s *APISuite) TestPutUploadOnePart(c *gc.C) {
	resp := httptesting.DoRequest(c, httptesting.DoRequestParams{
		Handler: s.srv,
		Method:  "POST",
		Do:      bakeryDo(s.idmServer.Client("bob")),
		URL:     storeURL("upload"),
	})
	var uploadResp params.NewUploadResponse
	err := json.Unmarshal(resp.Body.Bytes(), &uploadResp)
	c.Assert(err, gc.Equals, nil)

	part := "0123456789"
	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler: s.srv,
		Method:  "PUT",
		Do:      bakeryDo(s.idmServer.Client("bob")),
		URL:     storeURL("upload/" + uploadResp.UploadId + "/0?hash=" + hashOfString(part)),
		Body:    strings.NewReader(part),
	})

	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler: s.srv,
		Method:  "PUT",
		Do:      bakeryDo(s.idmServer.Client("bob")),
		URL:     storeURL("upload/" + uploadResp.UploadId),
		JSONBody: params.Parts{
			Parts: []params.Part{{
				Hash: hashOfString(part),
			}},
		},
		ExpectBody: &params.FinishUploadResponse{
			Hash: hashOfString(part),
		},
	})
}

func (s *APISuite) TestPutUploadParts(c *gc.C) {
	resp := httptesting.DoRequest(c, httptesting.DoRequestParams{
		Handler: s.srv,
		Do:      bakeryDo(s.idmServer.Client("bob")),
		Method:  "POST",
		URL:     storeURL("upload"),
	})
	var uploadResp params.NewUploadResponse
	err := json.Unmarshal(resp.Body.Bytes(), &uploadResp)
	c.Assert(err, gc.Equals, nil)

	part1 := newDataSource(1, 5*1024*1024)
	hash1, size1 := hashOf(part1)
	part1.Seek(0, 0)
	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler:       s.srv,
		Method:        "PUT",
		Do:            bakeryDo(s.idmServer.Client("bob")),
		ContentLength: size1,
		URL:           storeURL("upload/" + uploadResp.UploadId + "/0?hash=" + hash1),
		Body:          part1,
	})
	part2 := newDataSource(2, 5*1024*1024)
	hash2, size2 := hashOf(part2)
	part2.Seek(0, 0)
	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler:       s.srv,
		Method:        "PUT",
		Do:            bakeryDo(s.idmServer.Client("bob")),
		ContentLength: size2,
		URL:           storeURL("upload/" + uploadResp.UploadId + "/1?hash=" + hash2),
		Body:          part2,
	})

	part1.Seek(0, 0)
	part2.Seek(0, 0)

	hash, _ := hashOf(io.MultiReader(part1, part2))

	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler: s.srv,
		Method:  "PUT",
		Do:      bakeryDo(s.idmServer.Client("bob")),
		URL:     storeURL("upload/" + uploadResp.UploadId),
		JSONBody: params.Parts{
			Parts: []params.Part{{
				Hash: hash1,
			}, {
				Hash: hash2,
			}},
		},
		ExpectBody: &params.FinishUploadResponse{
			Hash: hash,
		},
	})
}

var uploadPartErrorTests = []struct {
	about           string
	url             string
	noContentLength bool
	expectError     string
	expectCode      params.ErrorCode
	expectStatus    int
	method          string
}{{
	about:        "missing hash",
	url:          storeURL("upload/someUploadId/0"),
	expectError:  "hash parameter not specified",
	expectCode:   params.ErrBadRequest,
	expectStatus: http.StatusBadRequest,
}, {
	about:           "missing content length",
	url:             storeURL("upload/someUploadId/0?hash=something"),
	noContentLength: true,
	expectError:     "Content-Length not specified",
	expectCode:      params.ErrBadRequest,
	expectStatus:    http.StatusBadRequest,
}, {
	about:        "negative part number",
	url:          storeURL("upload/someUploadId/-1?hash=something"),
	expectError:  "negative part number",
	expectCode:   params.ErrBadRequest,
	expectStatus: http.StatusBadRequest,
}, {
	about:        "bad part number",
	url:          storeURL("upload/someUploadId/x?hash=something"),
	expectError:  `bad part number "x"`,
	expectCode:   params.ErrBadRequest,
	expectStatus: http.StatusBadRequest,
}, {
	about:        "bad part number with trailing slash",
	url:          storeURL("upload/someUploadId/0/"),
	expectError:  "not found",
	expectCode:   params.ErrNotFound,
	expectStatus: http.StatusNotFound,
}, {
	about:        "extra element in url",
	url:          storeURL("upload/someUploadId/0/y/"),
	expectError:  "not found",
	expectCode:   params.ErrNotFound,
	expectStatus: http.StatusNotFound,
}, {
	about:        "missing part number",
	url:          storeURL("upload/someUploadId/?hash=something"),
	expectError:  `bad part number ""`,
	expectCode:   params.ErrBadRequest,
	expectStatus: http.StatusBadRequest,
}, {
	about:        "POST instead of PUT",
	url:          storeURL("upload/someUploadId/0?hash=something"),
	method:       "POST",
	expectError:  "POST not allowed",
	expectCode:   params.ErrMethodNotAllowed,
	expectStatus: http.StatusMethodNotAllowed,
}}

func (s *APISuite) TestPutUploadPartErrors(c *gc.C) {
	for i, test := range uploadPartErrorTests {
		c.Logf("test %d: %s", i, test.about)
		if test.method == "" {
			test.method = "PUT"
		}
		body := newDataSource(1, 0)
		contentLength := int64(-1)
		if !test.noContentLength {
			contentLength = 10
			body = newDataSource(1, 10)
		}
		httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
			Handler:       s.srv,
			Method:        test.method,
			Do:            bakeryDo(s.idmServer.Client("bob")),
			ContentLength: contentLength,
			URL:           test.url,
			Body:          body,
			ExpectStatus:  test.expectStatus,
			ExpectBody: params.Error{
				Message: test.expectError,
				Code:    test.expectCode,
			},
		})
	}
}

type dataSource struct {
	buf    []byte
	offset int64
	size   int64
}

// newDataSource returns a stream of size bytes holding
// a repeated number.
func newDataSource(fillWith int64, size int64) io.ReadSeeker {
	src := &dataSource{
		size: size,
	}
	for len(src.buf) < 8*1024 {
		src.buf = strconv.AppendInt(src.buf, fillWith, 10)
		src.buf = append(src.buf, ' ')
	}

	return src
}

func (s *dataSource) Seek(off int64, whence int) (int64, error) {
	switch whence {
	case 0:
		s.offset = off
	case 1:
		s.offset += off
	case 2:
		s.offset = s.size - off
	}
	return s.offset, nil
}

func (s *dataSource) Read(buf []byte) (int, error) {
	total := 0
	for len(buf) > 0 {
		if s.offset >= s.size {
			return total, io.EOF
		}
		bufIndex := int(s.offset % int64(len(s.buf)))
		n := copy(buf, s.buf[bufIndex:])
		s.offset += int64(n)
		buf = buf[n:]
		total += n
	}
	return total, nil
}
