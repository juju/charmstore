// Copyright 2017 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package v5 // import "gopkg.in/juju/charmstore.v5-unstable/internal/v5"

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/juju/httprequest"
	"gopkg.in/errgo.v1"
	"gopkg.in/juju/charmrepo.v2-unstable/csclient/params"

	"gopkg.in/juju/charmstore.v5-unstable/internal/blobstore"
)

const (
	defaultUploadExpiryDuration = 24 * time.Hour
	maxUploadExpiryDuration     = 24 * time.Hour
)

// POST /upload?expiry=expiry-duration
func (h *ReqHandler) serveUploadId(w http.ResponseWriter, req *http.Request) error {
	_, err := h.Authenticate(req)
	if err != nil {
		return errgo.Mask(err, errgo.Any)
	}
	switch req.Method {
	case "POST":
		expires := defaultUploadExpiryDuration
		if expiresStr := req.Form.Get("expires"); expiresStr != "" {
			exp, err := time.ParseDuration(expiresStr)
			if err != nil {
				return badRequestf(nil, "cannot parse expires %q", expiresStr)
			}
			if exp > maxUploadExpiryDuration {
				exp = maxUploadExpiryDuration
			}
			expires = exp
		}
		uploadId, err := h.Store.BlobStore.NewUpload(time.Now().Add(expires))
		if err != nil {
			return errgo.Mask(err)
		}
		return httprequest.WriteJSON(w, http.StatusOK, &params.UploadIdResponse{
			UploadId: uploadId,
		})
	default:
		return errgo.WithCausef(nil, params.ErrMethodNotAllowed, "%s not allowed", req.Method)
	}
}

// PUT /upload/upload-id/part-number
func (h *ReqHandler) serveUploadPart(w http.ResponseWriter, req *http.Request) error {
	// Make sure we consume the full request body, before responding.
	//
	// It seems a shame to require the whole, possibly large, part
	// is uploaded if we already know that the request is going to
	// fail, but it is necessary to prevent some failures.
	//
	// TODO: investigate using 100-Continue statuses to prevent
	// unnecessary uploads.
	defer io.Copy(ioutil.Discard, req.Body)
	_, err := h.Authenticate(req)
	if err != nil {
		return errgo.Mask(err, errgo.Any)
	}
	switch req.Method {
	case "PUT":
		elems := strings.Split(strings.TrimPrefix(req.URL.Path, "/"), "/")
		switch len(elems) {
		case 1:
			// PUT /upload/upload-id
			// Finish the upload.
			uploadId := elems[0]
			data, err := ioutil.ReadAll(req.Body)
			if err != nil {
				return errgo.Mask(err)
			}
			var pparts params.Parts
			if err := json.Unmarshal(data, &pparts); err != nil {
				return badRequestf(err, "cannot parse body")
			}
			parts := make([]blobstore.Part, len(pparts.Parts))
			for i := range pparts.Parts {
				parts[i] = blobstore.Part{
					Hash: pparts.Parts[i].Hash,
				}
			}
			_, hash, err := h.Store.BlobStore.FinishUpload(uploadId, parts)
			if err != nil {
				return errgo.Mask(err)
			}
			return httprequest.WriteJSON(w, http.StatusOK, params.FinishUploadResponse{
				Hash: hash,
			})
		case 2:
			// PUT /upload/upload-id/part-number
			// Upload a part.
			uploadId := elems[0]
			partNumberStr := elems[1]
			hash := req.Form.Get("hash")
			if hash == "" {
				return badRequestf(nil, "hash parameter not specified")
			}
			if req.ContentLength == -1 {
				return badRequestf(nil, "Content-Length not specified")
			}
			partNumber, err := strconv.Atoi(partNumberStr)
			if err != nil {
				return badRequestf(nil, "bad part number %q", partNumberStr)
			}
			err = h.Store.BlobStore.PutPart(uploadId, partNumber, req.Body, req.ContentLength, hash)
			if errgo.Cause(err) == blobstore.ErrBadParams {
				return errgo.WithCausef(err, params.ErrBadRequest, "")
			}
			if err != nil {
				return errgo.Mask(err)
			}
			return nil
		default:
			return errgo.WithCausef(nil, params.ErrNotFound, "")
		}
	case "DELETE":
		// TODO
		return errgo.New("delete not yet implemented")
	default:
		return errgo.WithCausef(nil, params.ErrMethodNotAllowed, "%s not allowed", req.Method)
	}
}
