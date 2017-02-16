// Copyright 2014 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package blobstore // import "gopkg.in/juju/charmstore.v5-unstable/internal/blobstore"

import (
	"crypto/sha512"
	"hash"
	"io"

	"github.com/juju/blobstore"
	"gopkg.in/errgo.v1"
	"gopkg.in/mgo.v2"
)

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
	db     *mgo.Database
	prefix string
	mstore blobstore.ManagedStorage
}

// New returns a new blob store that writes to the given database,
// prefixing its collections with the given prefix.
func New(db *mgo.Database, prefix string) *Store {
	rs := blobstore.NewGridFS(db.Name, prefix, db.Session)
	return &Store{
		db:     db,
		prefix: prefix,
		mstore: blobstore.NewManagedStorage(db, rs),
	}
}

// Put streams the content from the given reader into blob
// storage, with the provided name. The content should have the given
// size and hash.
func (s *Store) Put(r io.Reader, name string, size int64, hash string) error {
	return s.mstore.PutForEnvironmentAndCheckHash("", name, r, size, hash)
}

// Open opens the entry with the given name.
func (s *Store) Open(name string, index *MultipartIndex) (ReadSeekCloser, int64, error) {
	if index == nil {
		r, length, err := s.mstore.GetForEnvironment("", name)
		if err != nil {
			return nil, 0, errgo.Mask(err)
		}
		return r.(ReadSeekCloser), length, nil
	}
	return nil, 0, errgo.New("open of multipart blob not yet implemented")
	// TODO
	//	return &multipartReader{
	//		store: s,
	//		name: name,
	//		index: index,
	//	}
}

// Remove the given name from the Store.
func (s *Store) Remove(name string, index *MultipartIndex) error {
	return s.mstore.RemoveForEnvironment("", name)
}
