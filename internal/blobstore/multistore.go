// Copyright 2016 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package blobstore // import "gopkg.in/juju/charmstore.v5-unstable/internal/blobstore"

import (
	"io"

	"gopkg.in/errgo.v1"
)

// multiStore is a store.
var _ store = (*multiStore)(nil)

// multiStore holds the Stores to use as the blobstore.
type multiStore struct {
	stores []*Store
	// paralle arrays because i'm sloppy
	storeTypes []string
	storeModes []string
}

// NewMultiStore returns a new multiStore which is backed by
// the given ProviderConfig.
func NewMultiStore(bsps []ProviderConfig) *Store {
	return &Store{newMultiStore(bsps)}
}

func newMultiStore(bsps []ProviderConfig) *multiStore {
	s := &multiStore{
		stores:     make([]*Store, len(bsps)),
		storeTypes: make([]string, len(bsps)),
		storeModes: make([]string, len(bsps)),
	}
	for i, bsp := range bsps {
		s.storeTypes[i] = bsp.Type
		s.storeModes[i] = bsp.Mode
		if s.storeModes[i] == "" {
			s.storeModes[i] = "read-only"
		}
		switch bsp.Type {
		case "jujublobstore":
			s.stores[i] = NewBlobstoreFromProviderConfig(&bsp)
		case "s3":
			s.stores[i] = NewS3(&bsp)
		case "localfs":
			s.stores[i] = NewLocalFS(&bsp)
		default:
			panic("unknown provider-config type: " + bsp.Type)
		}
		logger.Debugf("%s blob storage provider configured by FaillbackStore: %v\n", bsp.Type, bsp)
	}
	return s
}

func (s *multiStore) Put(r io.Reader, name string, size int64, hash string, proof *ContentChallengeResponse) (cc *ContentChallenge, err error) {
	for i := range s.stores {
		if s.storeModes[i] == "read-write" || s.storeModes[i] == "write-only" {
			cc, err = s.stores[i].Put(r, name, size, hash, proof)
			if err != nil {
				logger.Errorf("multiStore PutUnchallenged %s in %s with hash %s err was: %v trying next(if available)", name, s.storeTypes[i], hash, err)
			}
		}
	}
	return
}

func (s *multiStore) PutUnchallenged(r io.Reader, name string, size int64, hash string) (err error) {
	for i := range s.stores {
		if s.storeModes[i] == "read-write" || s.storeModes[i] == "write-only" {
			err = s.stores[i].PutUnchallenged(r, name, size, hash)
			if err != nil {
				logger.Errorf("multiStore PutUnchallenged %s in %s with hash %s err was: %v trying next(if available)", name, s.storeTypes[i], hash, err)
			}
		}
	}
	return
}

func (s *multiStore) Open(name string) (ReadSeekCloser, int64, error) {
	for i := range s.stores {
		if s.storeModes[i] == "read-write" || s.storeModes[i] == "read-only" {
			f, st, err := s.stores[i].Open(name)
			if err == nil {
				return f, st, err
			}
			logger.Debugf("multiStore open %s not found in %s err was: %v trying next(if available)", name, s.storeTypes[i], err)
		}
	}
	return nil, 0, errgo.Newf("file not found %s", name)
}

func (s *multiStore) Remove(name string) (err error) {
	for i := range s.stores {
		if s.storeModes[i] == "read-write" || s.storeModes[i] == "write-only" {
			err = s.stores[0].Remove(name)
		}
	}
	return
}
