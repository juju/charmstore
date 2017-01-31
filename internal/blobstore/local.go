// Copyright 2016 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package blobstore // import "gopkg.in/juju/charmstore.v5-unstable/internal/blobstore"

import (
	"io"
	"os"
	"path"

	"github.com/juju/loggo"
)

var logger = loggo.GetLogger("charmstore.internal.blobstore")

type localFSStore struct {
	path string
}

func NewLocalFS(pc *ProviderConfig) *Store {
	return &Store{&localFSStore{path: pc.BucketName}}
}

func (s *localFSStore) Put(r io.Reader, name string, size int64, hash string, proof *ContentChallengeResponse) (_ *ContentChallenge, err error) {
	err = s.PutUnchallenged(r, name, size, hash)
	return
}

func (s *localFSStore) PutUnchallenged(r io.Reader, name string, size int64, hash string) error {
	w, err := os.Create(path.Join(s.path, name))
	if err != nil {
		logger.Errorf("localfs could not put %s", name)
		return err
	}
	io.Copy(w, r)
	logger.Debugf("localfs put %s", name)
	return nil
}

func (s *localFSStore) Open(name string) (ReadSeekCloser, int64, error) {
	f, err := os.Open(path.Join(s.path, name))
	if err != nil {
		logger.Errorf("localfs could not open %s", name)
		return nil, 0, err
	}
	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, 0, err
	}
	logger.Debugf("localfs openned %s", name)
	return f, stat.Size(), nil
}

func (s *localFSStore) Remove(name string) error {
	return os.Remove(name)
}
