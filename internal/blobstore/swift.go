// Copyright 2017 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package blobstore // import "gopkg.in/juju/charmstore.v5/internal/blobstore"

import (
	"fmt"
	"io"
	"strconv"

	"github.com/ncw/swift"
	"gopkg.in/errgo.v1"
)

// SwiftParams holds the configuration parameters for the swift backend.
type SwiftParams struct {
	// AuthURL holds the keystone authentication URL.
	AuthURL string

	// EndpointURL holds the URL of the swift endpoint.
	EndpointURL string

	// Bucket holds the swift container name to use to store all the blobs.
	Bucket string

	// Region holds the openstack region used in authentication.
	Region string

	// Secret holds the password or secret key used to authenticate.
	Secret string

	// Tenant holds the tenant name used in authentication.
	Tenant string

	// Username holds the name of the authenticating user.
	Username string
}

type swiftBackend struct {
	connection *swift.Connection
	container  string
}

// NewSwiftBackend returns a backend which uses OpenStack's Swift for
// its operations with the given credentials and auth mode. It stores
// all the data objects in the container with the given name.
func NewSwiftBackend(params SwiftParams) Backend {
	return &swiftBackend{
		connection: &swift.Connection{
			ApiKey:     params.Secret,
			AuthUrl:    params.AuthURL,
			Region:     params.Region,
			Tenant:     params.Tenant,
			UserName:   params.Username,
			StorageUrl: params.EndpointURL,
		},
		container: params.Bucket,
	}
}

func (s *swiftBackend) Get(name string) (r ReadSeekCloser, size int64, err error) {
	f, headers, err := s.connection.ObjectOpen(s.container, name, true, nil)
	if err != nil {
		if isNotFound(err) {
			return nil, 0, errgo.WithCausef(nil, ErrNotFound, "")
		}
		return nil, 0, errgo.Mask(err)
	}
	lengthstr := headers["Content-Length"]
	size, err = strconv.ParseInt(lengthstr, 10, 64)
	return swiftBackendReader{f}, size, errgo.Mask(err)
}

const maxBufferSize = 500 * 1024

func (s *swiftBackend) Put(name string, r io.Reader, size int64, hash string) error {
	// Ensure that we never try to write too much data.
	r = io.LimitReader(r, size)
	// Buffer the first few KiB in memory so that we can retry if there is an authentication failure.
	bufferSize := int64(maxBufferSize)
	if size < bufferSize {
		bufferSize = size
	}
	buf := make([]byte, size)
	if _, err := io.ReadFull(r, buf); err != nil {
		return errgo.Mask(err)
	}

	var f *swift.ObjectCreateFile
	h := NewHash()
	var err error
	for i := 0; i < 2; i++ {
		f, err = s.connection.ObjectCreate(s.container, name, true, "", "", swift.Headers{"Content-Length": fmt.Sprintf("%d", size)})
		if errgo.Cause(err) == swift.AuthorizationFailed {
			continue
		}
		if err != nil {
			break
		}
		w := io.MultiWriter(f, h)
		_, err = w.Write(buf)
		if errgo.Cause(err) == swift.AuthorizationFailed {
			f.Close()
			f = nil
			h.Reset()
			continue
		}
		if err != nil {
			break
		}
		// If we've written first part successfully we expect the remainder to complete.
		_, err = io.Copy(w, r)
		break
	}
	if f != nil {
		err1 := f.Close()
		if err == nil {
			err = err1
		}
	}
	if err != nil {
		return errgo.Mask(err)
	}
	if fmt.Sprintf("%x", h.Sum(nil)) != hash {
		err := s.connection.ObjectDelete(s.container, name)
		if err != nil {
			logger.Errorf("could not delete object from container after a hash mismatch was detected: %v", err)
		}
		return errgo.New("hash mismatch")
	}
	return nil
}

func (s *swiftBackend) Remove(name string) error {
	err := s.connection.ObjectDelete(s.container, name)
	if err != nil && isNotFound(err) {
		return errgo.WithCausef(nil, ErrNotFound, "")
	}
	return errgo.Mask(err)
}

// swiftBackendReader translates not-found errors as
// produced by Swift into not-found errors as expected
// by the Backend.Get interface contract.
type swiftBackendReader struct {
	ReadSeekCloser
}

func (r swiftBackendReader) Read(buf []byte) (int, error) {
	n, err := r.ReadSeekCloser.Read(buf)
	if err == nil || err == io.EOF {
		return n, err
	}
	if isNotFound(err) {
		return n, errgo.WithCausef(nil, ErrNotFound, "")
	}
	return n, errgo.Mask(err)
}

func isNotFound(err error) bool {
	err = errgo.Cause(err)
	return err == swift.ContainerNotFound || err == swift.ObjectNotFound
}
