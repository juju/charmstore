// Copyright 2017 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package blobstore // import "gopkg.in/juju/charmstore.v5-unstable/internal/blobstore"

import (
	"fmt"
	"io"
	"strconv"

	"github.com/juju/loggo"
	"gopkg.in/errgo.v1"
	"gopkg.in/goose.v2/client"
	"gopkg.in/goose.v2/errors"
	"gopkg.in/goose.v2/identity"
	"gopkg.in/goose.v2/swift"
)

type swiftStore struct {
	client    *swift.Client
	container string
}

// NewSwiftStore returns an ObjectStore which uses swift for its operations
// with the given credentials and auth mode.
func NewSwiftStore(cred *identity.Credentials, authmode identity.AuthMode, container string) ObjectStore {
	c := client.NewClient(cred,
		authmode,
		gooseLogger{},
	)
	return &swiftStore{
		client:    swift.New(c),
		container: container,
	}
}

func (s *swiftStore) Get(name string) (r ReadSeekCloser, size int64, err error) {
	r2, headers, err := s.client.GetReadSeeker(s.container, name)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil, 0, errgo.WithCausef(err, ErrNotFound, "")
		}
		return nil, 0, errgo.Mask(err)
	}
	lengthstr := headers.Get("Content-Length")
	size, err = strconv.ParseInt(lengthstr, 10, 64)
	return r2.(ReadSeekCloser), size, err
}

func (s *swiftStore) Put(name string, r io.Reader, size int64, hash string) error {
	h := NewHash()
	r2 := io.TeeReader(r, h)
	err := s.client.PutReader(s.container, name, r2, size)
	if err != nil {
		// TODO: investigate if PutReader can return err but the object still be
		// written. Should there be cleanup here?
		return err
	}
	if hash != fmt.Sprintf("%x", h.Sum(nil)) {
		err := s.client.DeleteObject(s.container, name)
		if err != nil {
			logger.Errorf("could not delete object from container after a hash mismatch was detected: %v", err)
		}
		return errgo.New("hash mismatch")
	}
	return nil
}

func (s *swiftStore) Remove(name string) error {
	err := s.client.DeleteObject(s.container, name)
	if err != nil && errors.IsNotFound(err) {
		return errgo.WithCausef(err, ErrNotFound, "")
	}
	return err
}

// gooseLogger implements the logger interface required
// by goose, using the loggo logger to do the actual
// logging.
// TODO: Patch goose to use loggo directly.
type gooseLogger struct{}

func (gooseLogger) Printf(f string, a ...interface{}) {
	logger.LogCallf(2, loggo.DEBUG, f, a...)
}
