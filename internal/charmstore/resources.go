// Copyright 2016 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package charmstore

import (
	"fmt"
	"io"
	"time"

	"gopkg.in/errgo.v1"
	"gopkg.in/juju/charm.v6-unstable"
	"gopkg.in/juju/charm.v6-unstable/resource"
	"gopkg.in/juju/charmrepo.v2-unstable/csclient/params"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"gopkg.in/juju/charmstore.v5-unstable/internal/mongodoc"
	"gopkg.in/juju/charmstore.v5-unstable/internal/router"
)

var resourceNotFound = errgo.Newf("resource not found")

// ListResources returns the set of resources for the charm. If the
// unpublished channel is specified then set is composed of the latest
// revision for each resource. Otherwise it holds the revisions declared
// when the charm/channel pair was published.
func (s Store) ListResources(entity *mongodoc.Entity, channel params.Channel) ([]*mongodoc.Resource, error) {
	if entity.URL.Series == "bundle" {
		return nil, errgo.Newf("bundles do not have resources")
	}
	if entity.CharmMeta == nil {
		return nil, errgo.Newf("entity missing charm metadata")
	}

	var docs []*mongodoc.Resource
	for name, meta := range entity.CharmMeta.Resources {
		doc, err := s.latestResource(entity, channel, name)
		if errgo.Cause(err) == resourceNotFound {
			// Convert the "not found" error into placeholder. We do
			// this instead of pre-populating the database with
			// placeholders. This keeps the machinery simpler while
			// still correct.
			doc = &mongodoc.Resource{
				CharmURL: entity.BaseURL,
				Name:     meta.Name,
				// Revision, Fingerprint, etc. are not set.
			}
		} else if err != nil {
			return nil, errgo.Notef(err, "failed to get resource %q", name)
		}
		docs = append(docs, doc)
	}
	mongodoc.SortResources(docs)
	return docs, nil
}

// ResourceInfo returns the info for the identified resource.
func (s Store) ResourceInfo(entity *mongodoc.Entity, name string, revision int) (*mongodoc.Resource, error) {
	if revision < 0 {
		return nil, errgo.New("revision cannot be negative")
	}
	doc, err := s.resource(entity.BaseURL, name, revision)
	if err != nil {
		return nil, errgo.Mask(err)
	}
	return doc, nil
}

// OpenResource returns the blob for the identified resource.
func (s Store) OpenResource(id *router.ResolvedURL, name string, revision int) (*mongodoc.Resource, io.ReadCloser, error) {
	if revision < 0 {
		return nil, nil, errgo.New("revision cannot be negative")
	}
	doc, err := s.resource(&id.URL, name, revision)
	if err != nil {
		return nil, nil, errgo.Mask(err, errgo.Is(resourceNotFound))
	}
	r, err := s.openResource(doc)
	if err != nil {
		return nil, nil, errgo.Mask(err)
	}
	return doc, r, nil
}

// OpenResource returns the blob for the latest revision of the identified resource.
func (s Store) OpenLatestResource(id *router.ResolvedURL, channel params.Channel, name string) (*mongodoc.Resource, io.ReadCloser, error) {
	entity, err := s.FindEntity(id, nil)
	if err != nil {
		return nil, nil, errgo.Mask(err, errgo.Is(params.ErrNotFound))
	}
	revision, err := s.latestResourceRevision(entity, channel, name)
	if err != nil {
		return nil, nil, errgo.Mask(err, errgo.Is(resourceNotFound))
	}
	doc, reader, err := s.OpenResource(id, name, revision)
	if err != nil {
		return nil, nil, errgo.Mask(err, errgo.Is(resourceNotFound))
	}
	return doc, reader, nil
}

func (s Store) openResource(doc *mongodoc.Resource) (io.ReadCloser, error) {
	r, size, err := s.BlobStore.Open(doc.BlobName)
	if err != nil {
		return nil, errgo.Notef(err, "cannot open resource data for %s", doc.Name)
	}
	if size != doc.Size {
		return nil, errgo.Newf("resource size mismatch")
	}
	// We would also verify that the hash matches if the hash were
	// readily available. However, it is not and comparing the size
	// is good enough.
	return r, nil
}

// ResourceBlob holds the information specific to a single resource blob.
type ResourceBlob struct {
	io.Reader

	//Fingerprint is the SHA-384 checksum of the blob.
	Fingerprint []byte

	// Size is the size of the blob in bytes.
	Size int64
}

// AddResource adds the resource to the resources collection and stores
// its blob.
func (s Store) AddResource(entity *mongodoc.Entity, name string, blob ResourceBlob) (revision int, err error) {
	revision, err = s.nextResourceRevision(entity, name)
	if err != nil {
		return -1, errgo.Mask(err)
	}

	if err := s.addResource(entity, name, blob, revision); err != nil {
		return -1, errgo.Mask(err)
	}
	return revision, nil
}

func (s Store) nextResourceRevision(entity *mongodoc.Entity, name string) (int, error) {
	latest, err := s.latestResourceRevision(entity, params.UnpublishedChannel, name)
	if errgo.Cause(err) == resourceNotFound {
		return 0, nil
	}
	if err != nil {
		return -1, errgo.Mask(err)
	}
	return latest + 1, nil
}

func (s Store) addResource(entity *mongodoc.Entity, name string, blob ResourceBlob, newRevision int) error {
	if !charmHasResource(entity.CharmMeta, name) {
		return errgo.Newf("charm does not have resource %q", name)
	}

	blobName, err := s.storeResource(blob)
	if err != nil {
		return errgo.Mask(err, errgo.Is(resourceNotFound))
	}

	doc := &mongodoc.Resource{
		CharmURL:    entity.BaseURL,
		Name:        name,
		Revision:    newRevision,
		Fingerprint: blob.Fingerprint,
		Size:        blob.Size,
		BlobName:    blobName,
		UploadTime:  time.Now().UTC(),
	}
	if s.insertResource(doc); err != nil {
		if err := s.BlobStore.Remove(blobName); err != nil {
			logger.Errorf("cannot remove blob %s after error: %v", blobName, err)
		}
		return errgo.Mask(err)
	}
	return nil
}

func (s Store) insertResource(doc *mongodoc.Resource) error {
	if err := doc.Validate(); err != nil {
		return errgo.Mask(err)
	}
	err := s.DB.Resources().Insert(doc)
	if err != nil {
		if mgo.IsDup(err) {
			return errgo.WithCausef(nil, params.ErrDuplicateUpload, "")
		}
		return errgo.Notef(err, "cannot insert resource")
	}
	return nil
}

func (s Store) storeResource(blob ResourceBlob) (string, error) {
	blobName := bson.NewObjectId().Hex()

	// Calculate the SHA384 hash while uploading the blob in the blob store.
	fpHash := resource.NewFingerprintHash()
	blobReader := io.TeeReader(blob, fpHash)

	// Upload the actual blob, and make sure that it is removed
	// if we fail later.
	hash := fmt.Sprintf("%x", blob.Fingerprint)
	err := s.BlobStore.PutUnchallenged(blobReader, blobName, blob.Size, hash)
	if err != nil {
		return "", errgo.Notef(err, "cannot put archive blob")
	}

	fp := fpHash.Fingerprint()
	if fp.String() != hash {
		if err := s.BlobStore.Remove(blobName); err != nil {
			logger.Errorf("cannot remove blob %s after error: %v", blobName, err)
		}
		return "", errgo.Newf("resource hash mismatch")
	}

	return blobName, nil
}

// RemoveResource deletes the resource from the collection and from the
// blob store. The resource revision must not be currently published.
// Otherwise it fails.
func (s Store) RemoveResource(entity *mongodoc.Entity, name string, revision int) error {
	if revision < 0 {
		// One alternative to failing here is to remove *all* revisions
		// of the resource.
		return errgo.New("revision cannot be negative")
	}
	// TODO(ericsnow) Ensure that the revision is not currently published.

	doc, err := s.resource(entity.BaseURL, name, revision)
	if err != nil {
		return errgo.WithCausef(nil, err, "")
	}

	query := mongodoc.NewResourceQuery(entity.BaseURL, name, revision)
	if err := s.DB.Resources().Remove(query); err != nil {
		if err == mgo.ErrNotFound {
			// Someone else got there first.
			err = params.ErrNotFound
		}
		return errgo.Mask(err, errgo.Is(params.ErrNotFound))
	}

	if err := s.BlobStore.Remove(doc.BlobName); err != nil {
		return errgo.Notef(err, "cannot remove blob %s", doc.BlobName)
	}
	return nil
}

// SetResource sets the revision of the identified resource for
// a specific charm revision in the given channel.
func (s Store) SetResource(entity *mongodoc.Entity, channel params.Channel, resName string, revision int) error {
	if channel == params.NoChannel {
		return errgo.New("missing channel")
	}
	if channel == params.UnpublishedChannel {
		return errgo.New("cannot publish to unpublished channel")
	}

	doc, err := s.resource(entity.BaseURL, resName, revision)
	if err != nil {
		return errgo.Mask(err, errgo.Is(resourceNotFound))
	}

	if !charmHasResource(entity.CharmMeta, doc.Name) {
		return errgo.Mask(err)
	}

	resourcesDoc, err := s.publishedResources(entity.URL, channel)
	if errgo.Cause(err) == resourceNotFound {
		// The charm/channel pair doesn't have a DB entry yet, so we
		// return a placeholder. As with ListResources(), we use a
		// placeholder instead of pre-populating the DB with the same
		// value.
		resourcesDoc = &mongodoc.Resources{
			CharmURL:  entity.URL,
			Channel:   channel,
			Revisions: make(map[string]int),
		}
	} else if err != nil {
		return errgo.Mask(err)
	}
	resourcesDoc.Revisions[resName] = revision

	query := mongodoc.NewResourcesQuery(entity.URL, channel)
	if _, err := s.DB.Resources().Upsert(query, resourcesDoc); err != nil {
		return errgo.Notef(err, "cannot set resource")
	}

	return nil
}

func (s Store) latestResource(entity *mongodoc.Entity, channel params.Channel, resName string) (*mongodoc.Resource, error) {
	revision, err := s.latestResourceRevision(entity, channel, resName)
	if err != nil {
		return nil, errgo.Mask(err, errgo.Is(resourceNotFound))
	}
	doc, err := s.resource(entity.URL, resName, revision)
	if err != nil {
		return nil, errgo.Mask(err, errgo.Is(resourceNotFound))
	}
	return doc, nil
}

func (s Store) latestResourceRevision(entity *mongodoc.Entity, channel params.Channel, resName string) (int, error) {
	if channel == params.UnpublishedChannel {
		docs, err := s.resources(entity.BaseURL, resName)
		if err != nil {
			return -1, errgo.Mask(err, errgo.Is(resourceNotFound))
		}
		if len(docs) == 0 {
			err := resourceNotFound
			return -1, errgo.WithCausef(nil, err, "")
		}
		mongodoc.SortResources(docs)
		latest := docs[len(docs)-1].Revision
		return latest, nil
	}

	doc, err := s.publishedResources(entity.URL, channel)
	if err != nil {
		return -1, errgo.Mask(err, errgo.Is(resourceNotFound))
	}
	latest, ok := doc.Revisions[resName]
	if !ok {
		// This means that a revision of the resource was not declared
		// when the charm/channel pair was published. One alternative
		// to returning "not found" would be to return the latest
		// unpublished revision. However, doing so would imply a
		// published revision when there actually wasn't one.
		err := resourceNotFound
		return -1, errgo.WithCausef(nil, err, "")
	}
	return latest, nil
}

func (s Store) resource(curl *charm.URL, resName string, revision int) (*mongodoc.Resource, error) {
	query := mongodoc.NewResourceQuery(curl, resName, revision)
	var doc mongodoc.Resource
	err := s.DB.Resources().Find(query).One(&doc)
	if err == mgo.ErrNotFound {
		err = resourceNotFound
		return nil, errgo.WithCausef(nil, err, "")
	}
	if err != nil {
		return nil, errgo.Mask(err)
	}
	if err := doc.Validate(); err != nil {
		return nil, errgo.Notef(err, "got bad data from DB")
	}
	return &doc, nil
}

func (s Store) resources(curl *charm.URL, resName string) ([]*mongodoc.Resource, error) {
	query := mongodoc.NewResourceQuery(curl, resName, -1)
	var docs []*mongodoc.Resource
	// All() populates an empty list when the query finds no results
	// rathar than returning mgo.ErrNotFound. So we check for an empty
	// list farther down.
	err := s.DB.Resources().Find(query).All(&docs)
	if err != nil {
		return nil, errgo.Mask(err)
	}
	if len(docs) == 0 {
		err = resourceNotFound
		return nil, errgo.WithCausef(nil, err, "")
	}
	for _, doc := range docs {
		if err := doc.Validate(); err != nil {
			return nil, errgo.Notef(err, "got bad data from DB")
		}
	}
	return docs, nil
}

func (s Store) publishedResources(curl *charm.URL, channel params.Channel) (*mongodoc.Resources, error) {
	if channel == params.NoChannel {
		return nil, errgo.New("missing channel")
	}
	if channel == params.UnpublishedChannel {
		return nil, errgo.Newf("%q channel not supported", channel)
	}

	query := mongodoc.NewResourcesQuery(curl, channel)
	var doc mongodoc.Resources
	err := s.DB.Resources().Find(query).One(&doc)
	if err == mgo.ErrNotFound {
		// Return a placeholder. See ListResources() for more on why we
		// use a placeholder.
		doc = mongodoc.Resources{
			CharmURL:  curl,
			Channel:   channel,
			Revisions: make(map[string]int),
		}
	} else if err != nil {
		return nil, errgo.Mask(err)
	}
	if err := doc.Validate(); err != nil {
		return nil, errgo.Notef(err, "got bad data from DB")
	}
	return &doc, nil
}

func charmHasResource(meta *charm.Meta, resName string) bool {
	for name := range meta.Resources {
		if resName == name {
			return true
		}
	}
	return false
}
