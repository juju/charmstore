// Copyright 2014-2017 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package blobstore // import "gopkg.in/juju/charmstore.v5-unstable/internal/blobstore"

import (
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"time"

	"github.com/juju/loggo"
	"github.com/rogpeppe/fastuuid"
	"gopkg.in/errgo.v1"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

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

const hashSize = sha512.Size384

// Backend represents the underlying data store used by blobstore.Store
// to store blob data.
type Backend interface {
	// Get gets a reader for the object with the given name
	// and its size. The returned reader should be closed after use.
	// If the object doesn't exist, an error with an ErrNotFound
	// cause should be returned.
	// If the object is removed while reading, the read
	// error's cause should be ErrNotFound.
	Get(name string) (r ReadSeekCloser, size int64, err error)

	// Put puts an object by reading its data from the given reader.
	// The data read from the reader should have the given
	// size and hex-encoded SHA384 hash.
	Put(name string, r io.Reader, size int64, hash string) error

	// Remove removes the object with the given name.
	Remove(name string) error
}

// blobRefDoc holds a mapping from blob hash to
// backend blob name.
type blobRefDoc struct {
	// Hash holds the hex-encoded hash of the blob.
	Hash string `bson:"_id"`
	// Name holds the name of the blob in the backend.
	Name string `bson:"name"`
	// PutTime stores the last time a new reference
	// was made to the blob with Put.
	PutTime time.Time
}

// Store stores data blobs in mongodb, de-duplicating by
// blob hash.
type Store struct {
	uploadc  *mgo.Collection
	blobRefc *mgo.Collection
	backend  Backend

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
func New(db *mgo.Database, prefix string, backend Backend) *Store {
	return &Store{
		uploadc:     db.C(prefix + ".upload"),
		blobRefc:    db.C(prefix + ".blobref"),
		backend:     backend,
		MinPartSize: defaultMinPartSize,
		MaxParts:    defaultMaxParts,
		MaxPartSize: defaultMaxPartSize,
	}
}

var uuidGen = fastuuid.MustNewGenerator()

// Put streams the content with the given hex-encoded SHA384
// hash, and size from the given reader into blob
// storage.
func (s *Store) Put(r io.Reader, hash string, size int64) error {
	if len(hash) != hashSize*2 {
		return errgo.Newf("implausible hash %q", hash)
	}
	_, err := s.blobRef(hash)
	if err != nil && errgo.Cause(err) != ErrNotFound {
		return errgo.Notef(err, "cannot get blob ref")
	}
	if err == nil {
		// The blob already exists. Update its PutTime
		// and check the hash. Note that we update its PutTime
		// immediately so that the blob isn't garbage collected
		// while we're checking the hash. This may be a potential
		// way for an attacker to extend the lifetime of a blob
		// that would otherwise be garbage collected, even when
		// they only have the hash but not the content, but
		// the utility of that as an attack vector seems very limited.
		err := s.updatePutTime(hash)
		if err == nil {
			// Check that the content actually matches its
			// purported hash.
			hasher := NewHash()
			rsize, err := io.Copy(hasher, r)
			if err != nil {
				return errgo.Notef(err, "cannot read blob content")
			}
			if rsize != size {
				return errgo.Notef(err, "unexpected blob size %d (expected %d)", rsize, size)
			}
			if fmt.Sprintf("%x", hasher.Sum(nil)) != hash {
				return errgo.Newf("blob hash mismatch")
			}
			// TODO update the PutTime if the upload has taken
			// a long time?
			return nil
		}
		if errgo.Cause(err) != mgo.ErrNotFound {
			return errgo.Notef(err, "cannot update put time")
		}
		// The blob has been garbage collected, so use
		// the usual put mechanism.
	}
	// Choose an arbitrary name for the blob (but include
	// some of the hash in there for debugging purposes)
	uuid := uuidGen.Next()
	name := fmt.Sprintf(hash[0:16] + "-" + fmt.Sprintf("%x", uuid[0:8]))
	if err := s.backend.Put(name, r, size, hash); err != nil {
		return errgo.Mask(err)
	}
	err = s.blobRefc.Insert(&blobRefDoc{
		Hash:    hash,
		Name:    name,
		PutTime: time.Now(),
	})
	if err == nil {
		return nil
	}
	if !mgo.IsDup(err) {
		// TODO delete blob from backend?
		return errgo.Notef(err, "cannot insert blob ref")
	}
	// The blob has already been put by some other
	// upload running concurrently, so delete the blob
	// we've just saved.
	if err := s.backend.Remove(name); err != nil {
		return errgo.Notef(err, "cannot remove blob %q after it was concurrently uploaded", name)
	}
	return nil
}

// Open opens the entry with the given hash. It returns an error
// with an ErrNotFound cause if the entry does not exist.
func (s *Store) Open(hash string, index *mongodoc.MultipartIndex) (ReadSeekCloser, int64, error) {
	if index != nil {
		return newMultiReader(s, index)
	}
	ref, err := s.blobRef(hash)
	if err != nil {
		return nil, 0, errgo.Mask(err, errgo.Is(ErrNotFound))
	}
	r, size, err := s.backend.Get(ref.Name)
	if err != nil {
		return nil, 0, errgo.NoteMask(err, "cannot get blob from backend", errgo.Is(ErrNotFound))
	}
	return r, size, nil
}

// GC runs the garbage collector, deleting all blobs not present in refs
// that have not been Put since the given time.
// Note that it also adds any internal blobs held by
// in-progress uploads to refs.
func (s *Store) GC(refs *Refs, before time.Time) error {
	if err := s.addUploadRefs(refs); err != nil {
		return errgo.Mask(err)
	}
	iter := s.blobRefc.Find(bson.D{{"puttime", bson.D{{"$lte", before}}}}).
		Select(bson.D{{"name", 1}}).
		Batch(5000).
		Iter()
	var doc blobRefDoc
	for iter.Next(&doc) {
		if refs.contains(doc.Hash) {
			continue
		}
		// Blob not found in refs, which means it's garbage
		// and should be collected right now.
		if err := s.blobRefc.Remove(bson.D{{
			"puttime", bson.D{{"$lte", before}},
		}, {
			"name", doc.Name,
		}}); err != nil {
			if err == mgo.ErrNotFound {
				// It's either been removed already
				// or it's just been referenced again
				// and its PutTime field updated.
				// In both cases, we don't need to
				// remove the blob.
				continue
			}
			return errgo.Notef(err, "cannot remove blobref entry")
		}
		if err := s.backend.Remove(doc.Name); err != nil {
			logger.Errorf("cannot remove garbage blob %q from backend (hash %q)", doc.Name, doc.Hash)
		}
		logger.Infof("removed garbage blob %q; hash %s", doc.Name, doc.Hash)
	}
	if err := iter.Close(); err != nil {
		return errgo.Notef(err, "cannot iterate over blobrefs")
	}
	return nil
}

// Refs holds information about the existence of
// a set of blob hashes.
type Refs struct {
	// TODO this implementation is good enough for up to a million
	// or so hashes (at the time of writing the number is ~45000),
	// but beyond that we might need to reconsider. One initial
	// mitigation without loss of precision would be to limit the
	// number of bytes used for each entry (even 4 or 8 bytes may be
	// sufficient, with a probe to check for false positives).
	refs map[[hashSize]byte]struct{}
}

// NewRefs returns a new Refs instance,
// using n as a hint for the number of references
// that will be added (the final number does not
// need to match this).
func NewRefs(n int) *Refs {
	return &Refs{
		refs: make(map[[hashSize]byte]struct{}, n),
	}
}

// Add records that the given hash is referenced.
// It ignores the hash if it's invalid.
func (r *Refs) Add(hash string) {
	data, ok := decodeHash(hash)
	if !ok {
		return
	}
	r.refs[data] = struct{}{}
}

// contains reports whether the given hash has been
// added to r.
func (r *Refs) contains(hash string) bool {
	data, ok := decodeHash(hash)
	if !ok {
		panic("invalid hash")
		return false
	}
	_, ok = r.refs[data]
	return ok
}

func (s *Store) updatePutTime(hash string) error {
	return s.blobRefc.UpdateId(hash, bson.D{{
		"$set", bson.D{{
			"puttime", time.Now(),
		}},
	}})
}

func (s *Store) blobRef(hash string) (*blobRefDoc, error) {
	var r blobRefDoc
	if err := s.blobRefc.FindId(hash).One(&r); err != nil {
		if err == mgo.ErrNotFound {
			return nil, errgo.WithCausef(nil, ErrNotFound, "")
		}
		return nil, errgo.Mask(err)
	}
	return &r, nil
}

// decodeHash decodes the hex-encoded hash
// and reports whether it has decoded successfully.
func decodeHash(hash string) ([hashSize]byte, bool) {
	if len(hash) != hashSize*2 {
		return [hashSize]byte{}, false
	}
	var data [48]byte
	if _, err := hex.Decode(data[:], []byte(hash)); err != nil {
		return [hashSize]byte{}, false
	}
	return data, true
}
