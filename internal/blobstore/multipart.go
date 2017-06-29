// Copyright 2017 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package blobstore

import (
	"encoding/base64"
	"fmt"
	"io"
	"time"

	"gopkg.in/errgo.v1"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"gopkg.in/juju/charmstore.v5-unstable/internal/mongodoc"
)

const (
	defaultMinPartSize = 5 * 1024 * 1024
	defaultMaxParts    = 400
	defaultMaxPartSize = 1<<32 - 1
)

// Part represents one part of a multipart blob.
type Part struct {
	Hash string
}

var ErrNotFound = errgo.New("blob not found")
var ErrBadParams = errgo.New("bad parameters")

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
	Parts []*PartInfo

	// Owner holds the owner of the upload - usually
	// the name of the entity that will refer to the
	// upload. This is set just before the entity
	// is updated to guard against the garbage collector
	// accidentally removing an upload because the
	// update process failed half-way through.
	Owner string `bson:",omitempty"`
}

// Note that the PartInfo type is also used as a document
// inside MongoDB - do not change without due care
// and attention.

// PartInfo holds information about one part of a multipart upload.
type PartInfo struct {
	// Hash holds the SHA384 hash of the part.
	Hash string
	// Size holds the size of the part.
	Size int64
	// Complete holds whether the part has been
	// successfully uploaded.
	Complete bool
}

// UploadInfo holds information on a given upload.
type UploadInfo struct {
	// Parts holds all the known parts of the upload.
	// Parts that haven't been uploaded yet will have nil
	// elements. Parts that are in progress or have been
	// aborted will have false Complete fields.
	Parts []*PartInfo

	// Expires holds when the upload will expire.
	Expires time.Time

	// Hash holds the hash of the entire upload.
	// This will be empty until the upload has
	// been completed with FinishUpload.
	Hash string `bson:"hash,omitempty"`
}

// Index returns a multipart index suitable for opening
// the multipart blob with the given info.
// It returns false if the info does not represent a completed
// upload.
func (info *UploadInfo) Index() (*mongodoc.MultipartIndex, bool) {
	if info.Hash == "" {
		return nil, false
	}
	idx := &mongodoc.MultipartIndex{
		Sizes: make([]uint32, len(info.Parts)),
	}
	for i, p := range info.Parts {
		idx.Sizes[i] = uint32(p.Size)
	}
	return idx, true
}

// NewUpload created a new multipart entry to track a multipart upload.
// It returns an uploadId that can be used to refer to it. After
// creating the upload, each part must be uploaded individually, and
// then the whole completed by calling FinishUpload and RemoveUpload.
func (s *Store) NewUpload(expires time.Time) (uploadId string, err error) {
	uploadId = base64.RawURLEncoding.EncodeToString([]byte(bson.NewObjectId()))
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
// If one of the parameters is badly formed, it returns an error with an ErrBadParams cause.
func (s *Store) PutPart(uploadId string, part int, r io.Reader, size int64, hash string) error {
	if part < 0 {
		return errgo.WithCausef(nil, ErrBadParams, "negative part number")
	}
	if part >= s.MaxParts {
		return errgo.WithCausef(nil, ErrBadParams, "part number %d too big (maximum %d)", part, s.MaxParts-1)
	}
	if size <= 0 {
		return errgo.WithCausef(nil, ErrBadParams, "non-positive part %d size %d", part, size)
	}
	if size >= s.MaxPartSize {
		return errgo.WithCausef(nil, ErrBadParams, "part %d too big (maximum %d)", part, s.MaxPartSize)
	}
	udoc, err := s.getUpload(uploadId)
	if err != nil {
		return errgo.Mask(err, errgo.Is(ErrNotFound))
	}
	if err := s.checkPartSizes(udoc.Parts, part, size); err != nil {
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
		// before we put the part so that RemoveExpiredParts
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
			PartInfo{
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
func (s *Store) checkPartSizes(parts []*PartInfo, part int, size int64) error {
	if part == -1 {
		// There's no current part, so pretend the last part
		// is being uploaded so that we don't complain about
		// it being too small.
		part = len(parts) - 1
	} else if part < len(parts)-1 && size < s.MinPartSize {
		return errgo.Newf("part too small (need at least %d bytes, got %d)", s.MinPartSize, size)
	}
	for i, p := range parts {
		if i != part && p != nil && p.Size < s.MinPartSize {
			return errgo.Newf("part %d was too small (need at least %d bytes, got %d)", i, s.MinPartSize, p.Size)
		}
	}
	return nil
}

// UploadInfo returns information on a given upload. It returns
// ErrNotFound if the upload has been deleted.
func (s *Store) UploadInfo(uploadId string) (UploadInfo, error) {
	udoc, err := s.getUpload(uploadId)
	if err != nil {
		return UploadInfo{}, errgo.Mask(err, errgo.Is(ErrNotFound))
	}
	return UploadInfo{
		Parts:   udoc.Parts,
		Expires: udoc.Expires,
		Hash:    udoc.Hash,
	}, nil
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
			"$set", bson.D{{partElem, PartInfo{
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
// deleted explicitly by calling RemoveUpload after the index data is
// stored.
func (s *Store) FinishUpload(uploadId string, parts []Part) (idx *mongodoc.MultipartIndex, hash string, err error) {
	udoc, err := s.getUpload(uploadId)
	if err != nil {
		return nil, "", errgo.Mask(err, errgo.Is(ErrNotFound))
	}
	if len(parts) != len(udoc.Parts) {
		return nil, "", errgo.Newf("part count mismatch (got %d but %d uploaded)", len(parts), len(udoc.Parts))
	}
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
	if err := s.checkPartSizes(udoc.Parts, -1, 0); err != nil {
		return nil, "", errgo.Mask(err)
	}
	// Calculate the hash of the entire thing, which marks
	// it as a completed upload.
	hash, err = s.setUploadHash(uploadId, udoc)
	if err != nil {
		if errgo.Cause(err) == ErrNotFound {
			return nil, "", errgo.New("upload expired or removed")
		}
		return nil, "", errgo.Mask(err)
	}
	idx = &mongodoc.MultipartIndex{
		Sizes:  make([]uint32, len(parts)),
		Hashes: make(mongodoc.Hashes, len(parts)),
	}
	for i := range udoc.Parts {
		idx.Sizes[i] = uint32(udoc.Parts[i].Size)
		idx.Hashes[i] = udoc.Parts[i].Hash
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
				return "", errgo.Mask(err, errgo.Is(ErrNotFound))
			}
		}
		hash = fmt.Sprintf("%x", h.Sum(nil))
	}
	// Note: setting the hash field marks the upload as complete.
	err := s.uploadc.UpdateId(uploadId, bson.D{{
		"$set", bson.D{{"hash", hash}},
	}})
	if err == mgo.ErrNotFound {
		return "", ErrNotFound
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
		return errgo.NoteMask(err, fmt.Sprintf("cannot open blob %q", name), errgo.Is(ErrNotFound))
	}
	defer rc.Close()
	if _, err := io.Copy(w, rc); err != nil {
		if errgo.Cause(err) == ErrNotFound {
			return errgo.WithCausef(err, ErrNotFound, "error reading blob %q", name)
		}
		return errgo.Notef(err, "error reading blob %q", name)
	}
	return nil
}

// SetOwner sets the "owner" of a given upload. This should be set just
// before the owner document is updated to refer to the upload.
// SetOwner will fail if the upload is already owned.
// The upload expiry deadline is also set to expires, so
// the caller has that much leeway to associate the upload id
// with some owner document.
func (s *Store) SetOwner(uploadId, owner string, expires time.Time) error {
	err := s.uploadc.Update(bson.D{
		{"_id", uploadId},
		{"hash", bson.D{{"$exists", true}}},
		{"$or", []bson.D{{{
			"owner", bson.D{{"$exists", false}},
		}}, {{
			"owner", owner,
		}}}},
	}, bson.D{{
		"$set", bson.D{
			{"owner", owner},
			{"expires", expires},
		},
	}})
	if err == nil {
		return nil
	}
	if err != mgo.ErrNotFound {
		return errgo.Notef(err, "cannot set owner")
	}
	// We don't know exactly why the update failed, so retrieve the
	// upload document so we can return a error that's not totally
	// ambiguous.
	udoc, err := s.getUpload(uploadId)
	switch {
	case errgo.Cause(err) == ErrNotFound:
		return errgo.WithCausef(nil, ErrNotFound, "upload has been removed")
	case err != nil:
		return errgo.Notef(err, "cannot get upload document")
	case udoc.Hash == "":
		return errgo.Newf("cannot set owner on incomplete upload")
	case udoc.Owner != "":
		return errgo.Newf("upload already used by something else")
	default:
		// Should never happen.
		return errgo.Newf("cannot set owner for some unknown reason")
	}
}

// RemoveExpiredUploads deletes any multipart entries that have passed
// their expiry date. If an expired upload is found with an owner, the
// given isOwnedBy function is called with the upload id and its current
// owner to make sure that it is not actually used - isOwnedBy should
// report whether the owner actually retains a reference to the given upload.
func (s *Store) RemoveExpiredUploads(isOwnedBy func(uploadId, owner string) (bool, error)) error {
	return s.removeExpiredUploads(isOwnedBy, time.Now())
}

func (s *Store) removeExpiredUploads(isOwnedBy func(uploadId, owner string) (bool, error), now time.Time) error {
	it := s.uploadc.Find(bson.D{
		{"expires", bson.D{{"$lt", now}}},
	}).Iter()
	defer it.Close()
	var udoc uploadDoc
	for it.Next(&udoc) {
		err := s.removeUpload(&udoc, isOwnedBy)
		if err != nil && errgo.Cause(err) != errUploadInUse {
			return errgo.Mask(err)
		}
	}
	if err := it.Err(); err != nil {
		return errgo.Mask(err)
	}
	return nil
}

// RemoveUpload deletes all the parts associated with the given upload
// id. It is a no-op if called twice on the same upload id. If the
// upload has an owner and isOwnedBy is not nil, isOwnedBy
// will be called with uploadId and the upload's current owner
// to determine be sure that it is not actually used.
// If isOwnedBy is nil, it is assumed to return true - i.e. the
// document is owned.
func (s *Store) RemoveUpload(uploadId string, isOwnedBy func(uploadId, owner string) (bool, error)) error {
	udoc, err := s.getUpload(uploadId)
	if err != nil {
		if errgo.Cause(err) == ErrNotFound {
			return nil
		}
		return errgo.Mask(err)
	}
	return s.removeUpload(udoc, isOwnedBy)
}

// removeUpload removes the upload document. It removes the parts
// associated with it if they're not in use.
func (s *Store) removeUpload(udoc *uploadDoc, isOwnedBy func(uploadId, owner string) (bool, error)) error {
	if udoc.Owner == "" {
		return s.removeUnownedUpload(udoc)
	}
	owned := true
	if isOwnedBy != nil {
		owned1, err := isOwnedBy(udoc.Id, udoc.Owner)
		if err != nil {
			return errgo.Notef(err, "cannot check blob ownership")
		}
		owned = owned1
	}
	return s.removeOwnedUpload(udoc, !owned)
}

// removeOwnedUpload removes an upload that has a non-empty
// owner field. If isOrphan is true, the upload's parts will be removed
// too.
func (s *Store) removeOwnedUpload(udoc *uploadDoc, isOrphan bool) error {
	if err := s.uploadc.RemoveId(udoc.Id); err != nil && err != mgo.ErrNotFound {
		return errgo.Mask(err)
	}
	if isOrphan {
		return s.removeParts(udoc)
	}
	return nil
}

// errUploadInUse is returned to signify that an
// upload document cannot be removed because
// it is in use.
var errUploadInUse = errgo.Newf("upload document is in use")

// removeUnownedUpload removes an upload that has an empty
// owner field. It returns errUploadInUse if the upload
// has become owned since the document was retrieved.
func (s *Store) removeUnownedUpload(udoc *uploadDoc) error {
	// It's possible that SetOwner has been called at the
	// same time as RemoveUpload, so only remove the upload
	// document if that hasn't happened.
	err := s.uploadc.Remove(bson.D{
		{"_id", udoc.Id},
		{"owner", bson.D{{"$exists", false}}},
	})
	switch err {
	case nil:
		return s.removeParts(udoc)
	case mgo.ErrNotFound:
		// Someone called SetOwner concurrently.
		return errUploadInUse
	default:
		return errgo.Mask(err)
	}
}

func (s *Store) removeParts(udoc *uploadDoc) error {
	var removeErr error
	for i := range udoc.Parts {
		name := uploadPartName(udoc.Id, i)
		err := s.Remove(name, nil)
		if err == nil || errgo.Cause(err) == ErrNotFound {
			// The blob *shouldn't* have been removed, but it's
			// probably best not to treat it as an error if it has.
			continue
		}
		if removeErr == nil {
			removeErr = err
		}
		logger.Errorf("cannot remove blob %q, leaving as garbage: %v", name, err)
	}
	if removeErr != nil {
		return errgo.Notef(removeErr, "failed to remove some data, see log for details")
	}
	return nil
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
