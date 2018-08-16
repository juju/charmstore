// Copyright 2017 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package blobstore // import "gopkg.in/juju/charmstore.v5/internal/blobstore"

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"

	"github.com/juju/loggo"
	"gopkg.in/errgo.v1"
	"gopkg.in/goose.v2/client"
	"gopkg.in/goose.v2/errors"
	"gopkg.in/goose.v2/identity"
	"gopkg.in/goose.v2/swift"
)

const maxBufferSize = 10 * 1024 * 1024

type swiftBackend struct {
	client    *swift.Client
	container string
	tmpdir    string
}

// NewSwiftBackend returns a backend which uses OpenStack's Swift for
// its operations with the given credentials and auth mode. It stores
// all the data objects in the container with the given name.
func NewSwiftBackend(cred *identity.Credentials, authmode identity.AuthMode, container, tmpdir string) Backend {
	c := client.NewClient(cred,
		authmode,
		gooseLogger{},
	)
	c.SetRequiredServiceTypes([]string{"object-store"})
	return &swiftBackend{
		client:    swift.New(c),
		container: container,
	}
}

func (s *swiftBackend) Get(name string) (r ReadSeekCloser, size int64, err error) {
	// Use infinite read-ahead here as the goose implementation of
	// byte range handling seems to differ from swift's.
	r2, headers, err := s.client.OpenObject(s.container, name, -1)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil, 0, errgo.WithCausef(nil, ErrNotFound, "")
		}
		return nil, 0, errgo.Mask(err)
	}
	lengthstr := headers.Get("Content-Length")
	size, err = strconv.ParseInt(lengthstr, 10, 64)
	return swiftBackendReader{r2.(ReadSeekCloser)}, size, err
}

func (s *swiftBackend) Put(name string, r io.Reader, size int64, hash string) error {
	if rs, ok := r.(io.ReadSeeker); ok {
		return errgo.Mask(s.putReadSeeker(name, rs, size, hash), errgo.Is(io.ErrUnexpectedEOF))
	}
	// Buffer the incoming data here. Goose would buffer it anyway so
	// doing it here gives us more control over how it is done.
	if size > maxBufferSize {
		return errgo.Mask(s.putFileBuffer(name, r, size, hash), errgo.Is(io.ErrUnexpectedEOF))
	}
	return errgo.Mask(s.putBuffer(name, r, size, hash), errgo.Is(io.ErrUnexpectedEOF))
}

func (s *swiftBackend) putReadSeeker(name string, rs io.ReadSeeker, size int64, hash string) error {
	if err := copyAndCheckHash(ioutil.Discard, rs, size, hash); err != nil {
		return errgo.Mask(err, errgo.Is(io.ErrUnexpectedEOF))
	}
	if _, err := rs.Seek(0, seekStart); err != nil {
		return errgo.Mask(err)
	}
	if err := s.client.PutReader(s.container, name, rs, size); err != nil {
		// TODO: investigate if PutReader can return err but the object still be
		// written. Should there be cleanup here?
		return errgo.Mask(err)
	}
	return nil
}

func (s *swiftBackend) putFileBuffer(name string, r io.Reader, size int64, hash string) error {
	fn := filepath.Join(s.tmpdir, name)
	f, err := os.OpenFile(fn, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return errgo.Mask(err)
	}
	defer f.Close()
	defer func() {
		if err := os.Remove(fn); err != nil {
			logger.Warningf("error removing temporary file: %s", err)
		}
	}()
	if err := copyAndCheckHash(f, r, size, hash); err != nil {
		return errgo.Mask(err, errgo.Is(io.ErrUnexpectedEOF))
	}
	if _, err := f.Seek(0, seekStart); err != nil {
		return errgo.Mask(err)
	}
	if err := s.client.PutReader(s.container, name, f, size); err != nil {
		// TODO: investigate if PutReader can return err but the object still be
		// written. Should there be cleanup here?
		return errgo.Mask(err)
	}
	return nil
}

func (s *swiftBackend) putBuffer(name string, r io.Reader, size int64, hash string) error {
	buf := bytes.NewBuffer(make([]byte, 0, size))
	if err := copyAndCheckHash(buf, r, size, hash); err != nil {
		return errgo.Mask(err, errgo.Is(io.ErrUnexpectedEOF))
	}
	if err := s.client.PutReader(s.container, name, bytes.NewReader(buf.Bytes()), size); err != nil {
		// TODO: investigate if PutReader can return err but the object still be
		// written. Should there be cleanup here?
		return errgo.Mask(err)
	}
	return nil
}

func (s *swiftBackend) Remove(name string) error {
	err := s.client.DeleteObject(s.container, name)
	if err != nil && errors.IsNotFound(err) {
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
	if errors.IsNotFound(err) {
		return n, errgo.WithCausef(nil, ErrNotFound, "")
	}
	return n, errgo.Mask(err)
}

// gooseLogger implements the logger interface required
// by goose, using the loggo logger to do the actual
// logging.
// TODO: Patch goose to use loggo directly.
type gooseLogger struct{}

func (gooseLogger) Printf(f string, a ...interface{}) {
	logger.LogCallf(2, loggo.DEBUG, f, a...)
}
