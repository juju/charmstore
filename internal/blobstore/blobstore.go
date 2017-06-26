// Copyright 2014-2017 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package blobstore // import "gopkg.in/juju/charmstore.v5-unstable/internal/blobstore"

import (
	"crypto/sha512"
	"hash"
	"io"

	"github.com/juju/errors"
	"github.com/juju/loggo"
	"gopkg.in/errgo.v1"
	"gopkg.in/mgo.v2"

	"gopkg.in/juju/charmstore.v5-unstable/internal/mongodoc"
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

// ObjectStore represents an object store. Even juju/blobstore is used like an
// object store.
type ObjectStore interface {
	// Get gets an object.
	Get(name string) (r ReadSeekCloser, size int64, err error)
	// Put puts an object.
	Put(name string, r io.Reader, size int64, hash string) error
	// Remove removes an object.
	Remove(name string) error
}

// Store stores data blobs in mongodb, de-duplicating by
// blob hash.
type Store struct {
	uploadc *mgo.Collection
	ostore  ObjectStore

	// The following fields are given default values by
	// New but may be changed away from the defaults
	// if desired.

	// MinPartSize holds the minimum size of a multipart upload part.
	MinPartSize int64

	// MaxPartSize holds the maximum size of a multipart upload part.
	MaxPartSize int64

	// MaxParts holds the maximum number of parts that there
	// can be in a multipart upload.
	MaxParts int
}

// New returns a new blob store that writes to the given database,
// prefixing its collections with the given prefix.
func New(db *mgo.Database, prefix string, ostore ObjectStore) *Store {
	return &Store{
		uploadc:     db.C(prefix + ".upload"),
		ostore:      ostore,
		MinPartSize: defaultMinPartSize,
		MaxParts:    defaultMaxParts,
		MaxPartSize: defaultMaxPartSize,
	}
}

// Put streams the content from the given reader into blob
// storage, with the provided name. The content should have the given
// size and hash.
func (s *Store) Put(r io.Reader, name string, size int64, hash string) error {
	return s.ostore.Put(name, r, size, hash)
}

// Open opens the entry with the given name. It returns an error
// with an ErrNotFound cause if the entry does not exist.
func (s *Store) Open(name string, index *mongodoc.MultipartIndex) (ReadSeekCloser, int64, error) {
	if index != nil {
		return newMultiReader(s, name, index)
	}
	r, size, err := s.ostore.Get(name)
	if err != nil {
		return nil, 0, errgo.Mask(err, errgo.Is(ErrNotFound))
	}
	return r, size, nil
}

// Remove the given name from the Store.
func (s *Store) Remove(name string, index *mongodoc.MultipartIndex) error {
	err := s.ostore.Remove(name)
	if errors.IsNotFound(err) {
		return errgo.WithCausef(err, ErrNotFound, "")
	}
	return err
}
