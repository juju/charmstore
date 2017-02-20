// Copyright 2014 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package blobstore // import "gopkg.in/juju/charmstore.v5-unstable/internal/blobstore"

import (
	"crypto/sha512"
	"hash"
	"io"

	"github.com/juju/blobstore"
	"github.com/juju/errors"
	"github.com/juju/loggo"
	"gopkg.in/errgo.v1"
	"gopkg.in/mgo.v2"
)

var logger = loggo.GetLogger("charmstore.internal.blobstore")

type ReadSeekCloser interface {
	io.Reader
	io.Seeker
	io.Closer
}

// NewHash is used to calculate checksums for the blob store.
func NewHash() hash.Hash {
	return sha512.New384()
}

// Store stores data blobs in mongodb, de-duplicating by
// blob hash.
type Store struct {
	uploadc     *mgo.Collection
	mstore      blobstore.ManagedStorage
	minPartSize int64
	maxParts    int
	maxPartSize int64
}

// New returns a new blob store that writes to the given database,
// prefixing its collections with the given prefix.
func New(db *mgo.Database, prefix string) *Store {
	rs := blobstore.NewGridFS(db.Name, prefix, db.Session)
	return &Store{
		uploadc:     db.C(prefix + ".upload"),
		mstore:      blobstore.NewManagedStorage(db, rs),
		minPartSize: defaultMinPartSize,
		maxParts:    defaultMaxParts,
		maxPartSize: defaultMaxPartSize,
	}
}

// Put streams the content from the given reader into blob
// storage, with the provided name. The content should have the given
// size and hash.
func (s *Store) Put(r io.Reader, name string, size int64, hash string) error {
	return s.mstore.PutForEnvironmentAndCheckHash("", name, r, size, hash)
}

// Open opens the entry with the given name. It returns an error
// with an ErrNotFound cause if the entry does not exist.
func (s *Store) Open(name string, index *MultipartIndex) (ReadSeekCloser, int64, error) {
	if index != nil {
		return newMultiReader(s, name, index)
	}
	r, length, err := s.mstore.GetForEnvironment("", name)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil, 0, errgo.WithCausef(err, ErrNotFound, "")
		}
		return nil, 0, errgo.Mask(err)
	}
	return r.(ReadSeekCloser), length, nil
}

// Remove the given name from the Store.
func (s *Store) Remove(name string, index *MultipartIndex) error {
	err := s.mstore.RemoveForEnvironment("", name)
	if errors.IsNotFound(err) {
		return errgo.WithCausef(err, ErrNotFound, "")
	}
	return errgo.Mask(err)
}
