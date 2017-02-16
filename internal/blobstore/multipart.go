// Copyright 2017 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package blobstore

import (
	"fmt"
	"io"
	"log"
	"time"

	"gopkg.in/errgo.v1"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

var (
	maxParts          = 400
	minPartSize int64 = 5 * 1024 * 1024
	maxPartSize int64 = 1<<32 - 1
)

// MultipartIndex holds the index of all the parts of a multipart blob.
// It should be stored in an external document along with the
// blob name so that the blob can be downloaded.
type MultipartIndex struct {
	Sizes []uint32
}

// Part represents one part of a multipart blob.
type Part struct {
	Hash string
}

var ErrNotFound = errgo.New("blob not found")

// uploadDoc describes the record that's held
// for a pending multipart upload.
type uploadDoc struct {
	// Id holds the upload id. The blob for each part in the
	// underlying blobstore will be named $id/$partnumber.
	Id string `bson:"_id"`

	// Hash holds the SHA384 hash of all the concatenated parts. It
	// is empty until after FinishUpload is called.
	Hash string `bson:"hash,omitempty"`

	// Expires holds the expiry time of the upload.
	Expires time.Time

	// Parts holds all the currently uploaded parts.
	Parts []*uploadPart
}

// uploadPart represents one part of an on-going upload.
type uploadPart struct {
	// Hash holds the SHA384 hash of the part.
	Hash string
	// Size holds the size of the part.
	Size int64
	// Complete holds whether the part has been
	// successfully uploaded.
	Complete bool
}

// NewUpload created a new multipart entry to track a multipart upload.
// It returns an uploadId that can be used to refer to it. After
// creating the upload, each part must be uploaded individually, and
// then the whole completed by calling FinishUpload and DeleteUpload.
func (s *Store) NewUpload(expires time.Time) (uploadId string, err error) {
	// TODO this makes an upload id that's 78 bytes.
	// A base64-encoded 256 bit random number would
	// only be 45 bytes and secure enough that we could
	// probably dispense with ownership checking.
	uploadId = fmt.Sprintf("%x", bson.NewObjectId())
	if err := s.uploadc.Insert(uploadDoc{
		Id:      uploadId,
		Expires: expires,
	}); err != nil {
		return "", errgo.Notef(err, "cannot create new upload")
	}
	return uploadId, nil
}

// PutPart uploads a part to the given upload id. The part number
// is specified with the part parameter; its content will be read from r
// and is expected to have the given size and hex-encoded SHA384 hash.
// A given part may not be uploaded more than once with a different hash.
//
// If the upload id was not found (for example, because it's expired),
// PutPart returns an error with an ErrNotFound cause.
func (s *Store) PutPart(uploadId string, part int, r io.Reader, size int64, hash string) error {
	if part < 0 {
		return errgo.Newf("negative part number")
	}
	if part >= maxParts {
		return errgo.Newf("part number %d too big (maximum %d)", part, maxParts-1)
	}
	if size <= 0 {
		return errgo.Newf("non-positive part %d size %d", part, size)
	}
	if size >= maxPartSize {
		return errgo.Newf("part %d too big (maximum %d)", part, maxPartSize)
	}
	udoc, err := s.getUpload(uploadId)
	if err != nil {
		return errgo.Mask(err, errgo.Is(ErrNotFound))
	}
	if err := checkPartSizes(udoc.Parts, part, size); err != nil {
		return errgo.Mask(err)
	}
	partElem := fmt.Sprintf("parts.%d", part)
	if part < len(udoc.Parts) && udoc.Parts[part] != nil {
		// There's already a (possibly complete) part record stored.
		p := udoc.Parts[part]
		if p.Hash != hash {
			return errgo.Newf("hash mismatch for already uploaded part")
		}
		if p.Complete {
			// It's already uploaded, then we can use the existing uploaded part.
			return nil
		}
		// Someone else made the part record, but it's not complete
		// perhaps because a previous part upload failed.
	} else {
		if udoc.Hash != "" {
			return errgo.Newf("cannot upload new part because upload is already complete")
		}
		// No part record. Make one, not marked as complete
		// before we put the part so that DeleteExpiredParts
		// knows to delete the part.
		if err := s.initializePart(uploadId, part, hash, size); err != nil {
			return errgo.Mask(err)
		}
	}
	// The part record has been updated successfully, so
	// we can actually upload the part now.
	partName := uploadPartName(uploadId, part)
	if err := s.Put(r, partName, size, hash); err != nil {
		return errgo.Notef(err, "cannot upload part %q", partName)
	}

	// We've put the part document, so we can now mark the part as
	// complete. Note: we update the entire part rather than just
	// setting $partElem.complete=true because of a bug in MongoDB
	// 2.4 which fails in that case.
	err = s.uploadc.UpdateId(uploadId, bson.D{{
		"$set", bson.D{{
			partElem,
			uploadPart{
				Hash:     hash,
				Size:     size,
				Complete: true,
			},
		}},
	}})
	if err != nil {
		return errgo.Notef(err, "cannot mark part as complete")
	}
	return nil
}

// checkPartSizes checks part sizes as much as we can.
// As the last part is allowed to be small, we can
// only check previously uploaded parts unless we're
// uploading an out-of-order part.
//
// The part argument holds the part being uploaded,
// or -1 if no part is currently being uploaded.
func checkPartSizes(parts []*uploadPart, part int, size int64) error {
	if part >= 0 && part < len(parts)-1 && size < minPartSize {
		return errgo.Newf("part too small (need at least %d bytes, got %d)", minPartSize, size)
	}
	for i, p := range parts {
		if i != part && p != nil && p.Size < minPartSize {
			return errgo.Newf("part %d was too small (need at least %d bytes, got %d)", i, minPartSize, p.Size)
		}
	}
	return nil
}

// ListUpload returns all the parts associated with the given
// upload id. It returns ErrNotFound if the upload has been
// deleted or finished.
func (s *Store) ListUpload(uploadId string) ([]Part, error) {
	return nil, errgo.New(" not implemented yet")
	// TODO
	// read multipart metadata
	// return parts from that, omitting parts that are currently in progress
}

// uploadPartName returns the blob name of the part with the given
// uploadId and part number.
func uploadPartName(uploadId string, part int) string {
	return fmt.Sprintf("%s/%d", uploadId, part)
}

// initializePart creates the initial record for a part.
func (s *Store) initializePart(uploadId string, part int, hash string, size int64) error {
	partElem := fmt.Sprintf("parts.%d", part)
	// Update the document if it's not been marked
	// as complete (it has no hash) and the part entry hasn't been
	// created yet.
	err := s.uploadc.Update(bson.D{
		{"_id", uploadId},
		{"hash", bson.D{{"$exists", false}}},
		{"$or", []bson.D{{{
			partElem, bson.D{{"$exists", false}},
		}}, {{
			partElem, nil,
		}}}},
	},
		bson.D{{
			"$set", bson.D{{partElem, uploadPart{
				Hash: hash,
				Size: size,
			}}},
		}},
	)
	if err == nil || err != mgo.ErrNotFound {
		return errgo.Mask(err)
	}
	return errgo.New("cannot update initial part record - concurrent upload of the same part?")
}

// FinishUpload completes a multipart upload by joining all the given
// parts into one blob. The resulting blob can be opened by passing
// uploadId and the returned multipart index to Open.
//
// The part numbers used will be from 0 to len(parts)-1.
//
// This does not delete the multipart metadata, which should still be
// deleted explicitly by calling DeleteUpload after the index data is
// stored.
func (s *Store) FinishUpload(uploadId string, parts []Part) (idx *MultipartIndex, hash string, err error) {
	udoc, err := s.getUpload(uploadId)
	if err != nil {
		return nil, "", errgo.Mask(err, errgo.Is(ErrNotFound))
	}
	if len(parts) != len(udoc.Parts) {
		return nil, "", errgo.Newf("part count mismatch (got %d but %d uploaded)", len(parts), len(udoc.Parts))
	}
	log.Printf("udoc.Parts: %#v", udoc.Parts)
	for i, p := range parts {
		pu := udoc.Parts[i]
		if pu == nil || !pu.Complete {
			return nil, "", errgo.Newf("part %d not uploaded yet", i)
		}
		if p.Hash != pu.Hash {
			return nil, "", errgo.Newf("hash mismatch on part %d (got %q want %q)", i, p.Hash, pu.Hash)
		}
	}
	// Even though some part size checking is done
	// when the parts are being uploaded, we still need
	// to check here in case several parts were uploaded
	// concurrently and one or more was too small.
	if err := checkPartSizes(udoc.Parts, -1, 0); err != nil {
		return nil, "", errgo.Mask(err)
	}
	// Calculate the hash of the entire thing, which marks
	// it as a completed upload.
	hash, err = s.setUploadHash(uploadId, udoc)
	if err != nil {
		return nil, "", errgo.Mask(err)
	}
	idx = &MultipartIndex{
		Sizes: make([]uint32, len(parts)),
	}
	for i := range udoc.Parts {
		idx.Sizes[i] = uint32(udoc.Parts[i].Size)
	}
	return idx, hash, nil
}

// setUploadHash calculates the hash of an complete multipart
// upload and sets it on the upload document which marks
// is as complete. It returns the hash.
// Precondition: all the parts have previously been checked for
// validity.
func (s *Store) setUploadHash(uploadId string, udoc *uploadDoc) (string, error) {
	if udoc.Hash != "" {
		return udoc.Hash, nil
	}
	var hash string
	if len(udoc.Parts) == 1 {
		// If there's only one part, we already know the hash
		// of the whole thing.
		hash = udoc.Parts[0].Hash
	} else {
		h := NewHash()
		for i := range udoc.Parts {
			if err := s.copyBlob(h, uploadPartName(uploadId, i)); err != nil {
				return "", errgo.Mask(err)
			}
		}
		hash = fmt.Sprintf("%x", h.Sum(nil))
	}
	// Note: setting the hash field marks the upload as complete.
	err := s.uploadc.UpdateId(uploadId, bson.D{{
		"$set", bson.D{{"hash", hash}},
	}})
	if err == mgo.ErrNotFound {
		return "", errgo.Newf("upload expired or removed")
	}
	if err != nil {
		return "", errgo.Notef(err, "could not update hash")
	}
	return hash, nil
}

// copyBlob copies the contents of blob with the given name
/// to the given Writer.
func (s *Store) copyBlob(w io.Writer, name string) error {
	rc, _, err := s.Open(name, nil)
	if err != nil {
		return errgo.Notef(err, "cannot open blob %q", name)
	}
	defer rc.Close()
	if _, err := io.Copy(w, rc); err != nil {
		return errgo.Notef(err, "cannot read blob %q", name)
	}
	return nil
}

// DeleteExpiredUploads deletes any multipart entries
// that have passed their expiry date.
func (s *Store) DeleteExpiredUploads() error {
	return errgo.New("not implemented yet")
}

// DeleteUpload deletes all the parts associated with the
// given upload id. It is a no-op if called twice on the
// same upload id.
func (s *Store) DeleteUpload(uploadId string) error {
	// TODO
	// read multipart metadata
	// delete all parts referenced in that
	// delete multipart metadata
	return errgo.New("not implemented yet")
}

func (s *Store) getUpload(uploadId string) (*uploadDoc, error) {
	var udoc uploadDoc
	if err := s.uploadc.FindId(uploadId).One(&udoc); err != nil {
		if err == mgo.ErrNotFound {
			return nil, errgo.WithCausef(nil, ErrNotFound, "upload id %q not found", uploadId)
		}
		return nil, errgo.Notef(err, "cannot get upload id %q", uploadId)
	}
	return &udoc, nil
}
