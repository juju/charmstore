// Copyright 2017 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package blobstore // import "gopkg.in/juju/charmstore.v5-unstable/internal/blobstore"

import (
	"io"

	"github.com/juju/blobstore"
	"github.com/juju/errors"
	"gopkg.in/errgo.v1"
	"gopkg.in/mgo.v2"
)

type mongoStore struct {
	blobstore.ManagedStorage
}

// NewMongoStore returns an ObjectStore which uses mongodb gridfs for its
// operations with the given database and gridfs prefix. This uses a
// "ManagedStorage" layer on top of gridfs from github.com/juju/blobstore.
func NewMongoStore(db *mgo.Database, prefix string) ObjectStore {
	rs := blobstore.NewGridFS(db.Name, prefix, db.Session)
	return &mongoStore{
		ManagedStorage: blobstore.NewManagedStorage(db, rs),
	}
}

func (m *mongoStore) Get(container, name string) (ReadSeekCloser, int64, error) {
	r, s, err := m.GetForEnvironment(container, name)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil, 0, errgo.WithCausef(err, ErrNotFound, "")
		}
		return nil, 0, err
	}
	return r.(ReadSeekCloser), s, nil
}

func (m *mongoStore) Put(container, name string, r io.Reader, size int64, hash string) error {
	return m.PutForEnvironmentAndCheckHash(container, name, r, size, hash)
}

func (m *mongoStore) Remove(container, name string) error {
	return m.RemoveForEnvironment(container, name)
}
