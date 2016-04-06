// Copyright 2016 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package charmstore

import (
	"io"
	"time"

	"gopkg.in/errgo.v1"
	"gopkg.in/juju/charm.v6-unstable"
	"gopkg.in/juju/charmrepo.v2-unstable/csclient/params"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"gopkg.in/juju/charmstore.v5-unstable/internal/mongodoc"
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

// ResourceBlob holds the information specific to a single resource blob.
type ResourceBlob struct {
	io.Reader

	//Fingerprint is the SHA-384 checksum of the blob.
	Fingerprint []byte

	// Size is the size of the blob in bytes.
	Size int64
}

// TODO(ericsnow) We will need Store.nextResourceRevision() to get the
// value to pass to addResource().

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
	name := bson.NewObjectId().Hex()
	// TODO(ericsnow) We will finish this in a follow-up patch.
	return name, nil
}

func (s Store) setResource(entity *mongodoc.Entity, channel params.Channel, resName string, revision int) error {
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
			return -1, errgo.WithCausef(err, err, "")
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
		return -1, errgo.WithCausef(err, err, "")
	}
	return latest, nil
}

func (s Store) resource(curl *charm.URL, resName string, revision int) (*mongodoc.Resource, error) {
	query := mongodoc.NewResourceQuery(curl, resName, revision)
	var doc mongodoc.Resource
	err := s.DB.Resources().Find(query).One(&doc)
	if err == mgo.ErrNotFound {
		err = resourceNotFound
		return nil, errgo.WithCausef(err, err, "")
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
		return nil, errgo.WithCausef(err, err, "")
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
