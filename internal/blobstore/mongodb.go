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

type mongoBackend struct {
	blobstore.ManagedStorage
}

// NewMongoBackend returns a backend implementation which stores
// data in the given MongoDB database, using prefix as a prefix for
// the collections created.
func NewMongoBackend(db *mgo.Database, prefix string) Backend {
	rs := blobstore.NewGridFS(db.Name, prefix, db.Session)
	return &mongoBackend{
		ManagedStorage: blobstore.NewManagedStorage(db, rs),
	}
}

func (m *mongoBackend) Get(name string) (ReadSeekCloser, int64, error) {
	r, s, err := m.GetForEnvironment("", name)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil, 0, errgo.WithCausef(err, ErrNotFound, "")
		}
		return nil, 0, err
	}
	return r.(ReadSeekCloser), s, nil
}

func (m *mongoBackend) Put(name string, r io.Reader, size int64, hash string) error {
	return m.PutForEnvironmentAndCheckHash("", name, r, size, hash)
}

func (m *mongoBackend) Remove(name string) error {
	return m.RemoveForEnvironment("", name)
}
