// Copyright 2017 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package blobstore // import "gopkg.in/juju/charmstore.v5-unstable/internal/blobstore"

import (
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/juju/loggo"
	"gopkg.in/errgo.v1"
	"gopkg.in/goose.v2/client"
	"gopkg.in/goose.v2/errors"
	"gopkg.in/goose.v2/identity"
	"gopkg.in/goose.v2/swift"
)

type swiftStore struct {
	*swift.Client
}

// NewSwiftStore returns an ObjectStore which uses swift for its operations
// with the given credentials and auth mode.
func NewSwiftStore(cred *identity.Credentials, authmode identity.AuthMode) ObjectStore {
	c := client.NewClient(cred,
		authmode,
		&adaptedLogger{logger})
	return &swiftStore{
		Client: swift.New(c),
	}
}

func (s *swiftStore) Get(container, name string) (r ReadSeekCloser, size int64, err error) {
	r2, headers, err := s.GetReadSeeker(container, name)
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

func (s *swiftStore) Put(container, name string, r io.Reader, size int64, hash string) error {
	h := NewHash()
	r2 := io.TeeReader(r, h)
	err := s.PutReader(container, name, r2, size)
	if err != nil {
		// Keep juju/bloblstore semantics here.
		if strings.HasPrefix(err.(errors.Error).Cause().Error(), "failed reading the request data") {
			return errgo.New("hash mismatch")
		}
		// TODO: investigate if PutReader can return err but the object still be
		// written. Should there be cleanup here?
		return err
	}
	if hash != fmt.Sprintf("%x", h.Sum(nil)) {
		err := s.DeleteObject(container, name)
		if err != nil {
			logger.Errorf("could not delete object from container after a hash mismatch was detected: %v", err)
		}
		return errgo.New("hash mismatch")
	}
	return nil
}

func (s *swiftStore) Remove(container, name string) error {
	err := s.DeleteObject(container, name)
	if err != nil && errors.IsNotFound(err) {
		return errgo.WithCausef(err, ErrNotFound, "")
	}
	return err
}

// adaptedLogger allows goose to log, but the trace shows from here instead of
// the correct place in goose. TODO: Patch goose to use loggo directly.
type adaptedLogger struct {
	loggo.Logger
}

func (al *adaptedLogger) Printf(f string, a ...interface{}) {
	al.LogCallf(2, loggo.DEBUG, f, a...)
}
