// Copyright 2016 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package charmstore // import "gopkg.in/juju/charmstore.v5-unstable/internal/charmstore"

import (
	"io"

	"gopkg.in/errgo.v1"
	"gopkg.in/juju/charm.v6-unstable"
	"gopkg.in/juju/charm.v6-unstable/resource"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"gopkg.in/juju/charmstore.v5-unstable/internal/mongodoc"
)

var resourceNotFound = errgo.Newf("resource not found")

// ListResources returns the list of resources for the charm at the
// latest revision for each resource.
func (s Store) ListResources(entity *mongodoc.Entity) ([]resource.Resource, error) {
	if entity.URL.Series == "bundle" {
		return nil, errgo.Newf("bundles do not have resources")
	}
	if entity.CharmMeta == nil {
		return nil, errgo.Newf("entity missing charm metadata")
	}

	var resources []resource.Resource
	for name, meta := range entity.CharmMeta.Resources {
		res, err := s.latestResource(entity, name)
		if err == resourceNotFound {
			// TODO(ericsnow) Fail? At least a dummy resource *must* be
			// in charm store?
			// We default to upload and set it to "store" once the resource
			// has been uploaded to the store.
			resOrigin := resource.OriginUpload
			res = resource.Resource{
				Meta:   meta,
				Origin: resOrigin,
				// Revision, Fingerprint, and Size are not set.
			}
		} else if err != nil {
			return nil, errgo.Notef(err, "failed to get resource")
		}
		resources = append(resources, res)
	}
	resource.Sort(resources)
	return resources, nil
}

func (s Store) latestResource(entity *mongodoc.Entity, resName string) (resource.Resource, error) {
	revision, err := s.latestResourceRevision(entity, resName)
	if err != nil {
		return resource.Resource{}, err
	}
	// TODO(ericsnow) We need to pass in a base ID...
	res, _, err := s.resource(entity.URL, resName, revision)
	return res, err
}

func (s Store) latestResourceRevision(entity *mongodoc.Entity, resName string) (int, error) {
	latest, ok := entity.Resources[resName]
	if !ok {
		// TODO(ericsnow) Fail if the resource otherwise exists?
		return -1, resourceNotFound
	}
	return latest, nil
}

func (s Store) resource(curl *charm.URL, resName string, revision int) (res resource.Resource, blobname string, err error) {
	var doc mongodoc.Resource
	id := mongodoc.NewResourceID(curl, resName, revision)
	err = s.DB.Resources().FindId(id).One(&doc)
	if err == mgo.ErrNotFound {
		// TODO(ericsnow) Fail because "latest" points to a missing resource?
		err = resourceNotFound
	}
	if err != nil {
		return resource.Resource{}, "", err
	}
	res, err = mongodoc.Doc2Resource(doc)
	if err != nil {
		return res, "", errgo.Notef(err, "failed to convert resource doc")
	}
	return res, doc.BlobName, nil
}

func (s Store) resourceDoc(curl *charm.URL, resName string, revision int) (mongodoc.Resource, error) {
	var doc mongodoc.Resource
	id := mongodoc.NewResourceID(curl, resName, revision)
	err := s.DB.Resources().FindId(id).One(&doc)
	if err == mgo.ErrNotFound {
		// TODO(ericsnow) Fail because "latest" points to a missing resource?
		err = resourceNotFound
	}
	return doc, err
}

func (s Store) addResource(entity *mongodoc.Entity, res resource.Resource, blob io.Reader, newRevision int) error {
	blobName, err := s.storeResource(entity, res, blob)
	if err := checkCharmResource(entity, res); err != nil {
		return err
	}
	if s.insertResource(entity, res, blobName, newRevision); err != nil {
		if err := s.BlobStore.Remove(blobName); err != nil {
			logger.Errorf("cannot remove blob %s after error: %v", blobName, err)
		}
		return err
	}
	return nil
}

func (s Store) insertResource(entity *mongodoc.Entity, res resource.Resource, blobName string, newRevision int) error {
	res.Revision = newRevision
	if err := checkCharmResource(entity, res); err != nil {
		return err
	}
	// TODO(ericsnow) We need to pass in a base ID...
	doc, err := mongodoc.Resource2Doc(entity.URL, res)
	if err != nil {
		return err
	}
	doc.BlobName = blobName

	err = s.DB.Resources().Insert(doc)
	if err != nil && !mgo.IsDup(err) {
		return errgo.Notef(err, "cannot insert resource")
	}

	return nil
}

func (s Store) storeResource(entity *mongodoc.Entity, res resource.Resource, blob io.Reader) (string, error) {
	name := bson.NewObjectId().Hex()
	// TODO(ericsnow) We will finish this in a follow-up patch.
	return name, nil
}

// TODO(ericsnow) We will need Store.nextResourceRevision()...

func (s Store) setResource(entity *mongodoc.Entity, resName string, revision int) error {
	// TODO(ericsnow) We need to pass in a base ID...
	res, _, err := s.resource(entity.URL, resName, revision)
	if err != nil {
		return err
	}
	if err := checkCharmResource(entity, res); err != nil {
		return err
	}

	resources := entity.Resources
	if resources == nil {
		resources = make(map[string]int)
	}
	resources[resName] = revision

	resolvedURL := EntityResolvedURL(entity)
	err = s.UpdateEntity(resolvedURL, bson.D{
		{"$set", bson.D{
			{"resources", resources},
		}},
	})
	if err != nil {
		return err
	}

	return nil
}

// checkCharmResource ensures that the given entity is okay
// to associate with a revisioned resource.
func checkCharmResource(entity *mongodoc.Entity, res resource.Resource) error {
	// TODO(ericsnow) Verify that the revisioned resources is in the DB.

	if err := res.Validate(); err != nil {
		return err
	}
	if res.Fingerprint.IsZero() {
		return errgo.Newf("resources must have a fingerprint")
	}

	if entity.URL.Series == "bundle" {
		return errgo.Newf("bundles do not have resources")
	}
	if !charmHasResource(entity.CharmMeta, res.Name) {
		return errgo.Newf("charm does not have resource %q", res.Name)
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
