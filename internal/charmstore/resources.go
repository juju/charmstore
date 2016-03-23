// Copyright 2016 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package charmstore // import "gopkg.in/juju/charmstore.v5-unstable/internal/charmstore"

import (
	"io"

	"gopkg.in/errgo.v1"
	"gopkg.in/juju/charm.v6-unstable"
	"gopkg.in/juju/charmrepo.v2-unstable/csclient/params"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"gopkg.in/juju/charmstore.v5-unstable/internal/mongodoc"
)

var resourceNotFound = errgo.Newf("resource not found")

// ListResources returns the list of resources for the charm at the
// latest revision for each resource.
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
		if err == resourceNotFound {
			// TODO(ericsnow) Fail? At least a dummy resource *must* be
			// in charm store?
			// We default to upload and set it to "store" once the resource
			// has been uploaded to the store.
			doc = &mongodoc.Resource{
				CharmURL: entity.BaseURL,
				Name:     meta.Name,
				// Revision, Fingerprint, etc. are not set.
			}
		} else if err != nil {
			return nil, errgo.Notef(err, "failed to get resource")
		}
		docs = append(docs, doc)
	}
	mongodoc.SortResources(docs)
	return docs, nil
}

// TODO(ericsnow) We will need Store.nextResourceRevision() to get the
// value to pass to addResource().

func (s Store) addResource(entity *mongodoc.Entity, doc *mongodoc.Resource, blob io.Reader, newRevision int) error {
	copied := *doc
	doc = &copied
	blobName, err := s.storeResource(entity, doc, blob)
	if err := checkCharmResource(entity, doc); err != nil {
		return err
	}
	doc.BlobName = blobName
	doc.Revision = newRevision
	if s.insertResource(entity, doc); err != nil {
		if err := s.BlobStore.Remove(doc.BlobName); err != nil {
			logger.Errorf("cannot remove blob %s after error: %v", doc.BlobName, err)
		}
		return err
	}
	return nil
}

func (s Store) insertResource(entity *mongodoc.Entity, doc *mongodoc.Resource) error {
	if err := checkCharmResource(entity, doc); err != nil {
		return err
	}

	err := s.DB.Resources().Insert(doc)
	if err != nil && !mgo.IsDup(err) {
		return errgo.Notef(err, "cannot insert resource")
	}
	// TODO(ericsnow) Also fail for dupe?

	return nil
}

func (s Store) storeResource(entity *mongodoc.Entity, doc *mongodoc.Resource, blob io.Reader) (string, error) {
	name := bson.NewObjectId().Hex()
	// TODO(ericsnow) We will finish this in a follow-up patch.
	return name, nil
}

func (s Store) setResource(entity *mongodoc.Entity, channel params.Channel, resName string, revision int) error {
	doc, err := s.resource(entity.URL, resName, revision)
	if err != nil {
		return err
	}

	if err := checkCharmResource(entity, doc); err != nil {
		return err
	}

	resourcesDoc, err := s.resources(channel, entity.URL)
	if err != nil {
		return err
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
		return nil, err
	}
	doc, err := s.resource(entity.URL, resName, revision)
	return doc, err
}

func (s Store) latestResourceRevision(entity *mongodoc.Entity, channel params.Channel, resName string) (int, error) {
	doc, err := s.resources(channel, entity.URL)
	if err != nil {
		return -1, err
	}
	latest, ok := doc.Revisions[resName]
	if !ok {
		// TODO(ericsnow) Fail if the resource otherwise exists?
		return -1, resourceNotFound
	}
	return latest, nil
}

func (s Store) resource(curl *charm.URL, resName string, revision int) (*mongodoc.Resource, error) {
	query := mongodoc.NewResourceQuery(curl, resName, revision)
	var doc mongodoc.Resource
	err := s.DB.Resources().Find(query).One(&doc)
	if err == mgo.ErrNotFound {
		err = resourceNotFound
	}
	if err != nil {
		return nil, err
	}
	if err := doc.Validate(); err != nil {
		return nil, errgo.Notef(err, "got bad data from DB")
	}
	return &doc, nil
}

func (s Store) resources(channel params.Channel, curl *charm.URL) (*mongodoc.Resources, error) {
	query := mongodoc.NewResourcesQuery(curl, channel)
	var doc mongodoc.Resources
	err := s.DB.Resources().Find(query).One(&doc)
	if err == mgo.ErrNotFound {
		doc = mongodoc.Resources{
			CharmURL:  curl,
			Channel:   channel,
			Revisions: make(map[string]int),
		}
	} else if err != nil {
		return nil, err
	}
	if err := doc.Validate(); err != nil {
		return nil, errgo.Notef(err, "got bad data from DB")
	}
	return &doc, nil
}

// checkCharmResource ensures that the given entity is okay
// to associate with a revisioned resource.
func checkCharmResource(entity *mongodoc.Entity, doc *mongodoc.Resource) error {
	// TODO(ericsnow) Verify that the revisioned resource is in the DB.

	if err := doc.Validate(); err != nil {
		return err
	}

	if !charmHasResource(entity.CharmMeta, doc.Name) {
		return errgo.Newf("charm does not have resource %q", doc.Name)
	}

	return nil
}

func charmHasResource(meta *charm.Meta, resName string) bool {
	for name := range meta.Resources {
		if resName == name {
			return true
		}
	}
	return false
}
