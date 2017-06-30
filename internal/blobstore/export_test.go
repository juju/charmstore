package blobstore

import (
	"gopkg.in/mgo.v2"
)

type UploadDoc uploadDoc

func BackendGridFS(s *Store) *mgo.GridFS {
	return s.backend.(*mongoBackend).fs
}
