// Copyright 2016 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package charmstore // import "gopkg.in/juju/charmstore.v5-unstable/internal/charmstore"

import (
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
	revision, ok := entity.Resources[resName]
	if !ok {
		// TODO(ericsnow) Fail if the resource otherwise exists?
		return resource.Resource{}, resourceNotFound
	}
	// TODO(ericsnow) We need to pass in a base ID...
	return s.resource(entity.URL, resName, revision)
}

func (s Store) resource(curl *charm.URL, resName string, revision int) (resource.Resource, error) {
	var res resource.Resource

	var doc mongodoc.Resource
	id := mongodoc.NewResourceID(curl, resName, revision)
	err := s.DB.Resources().FindId(id).One(&doc)
	if err == mgo.ErrNotFound {
		// TODO(ericsnow) Fail because "latest" points to a missing resource?
		err = resourceNotFound
	}
	if err != nil {
		return res, err
	}

	res, err = mongodoc.Doc2Resource(doc)
	if err != nil {
		return res, errgo.Notef(err, "failed to convert resource doc")
	}

	return res, nil
}

func (s Store) insertResource(entity *mongodoc.Entity, res resource.Resource, newRevision int) error {
	res.Revision = newRevision
	if err := mongodoc.CheckCharmResource(entity, res); err != nil {
		return err
	}
	// TODO(ericsnow) We need to pass in a base ID...
	doc, err := mongodoc.Resource2Doc(entity.URL, res)
	if err != nil {
		return err
	}

	err = s.DB.Resources().Insert(doc)
	if err != nil && !mgo.IsDup(err) {
		return errgo.Notef(err, "cannot insert resource")
	}

	return nil
}

// TODO(ericsnow) We will need Store.nextResourceRevision()...

func (s Store) setResource(entity *mongodoc.Entity, resName string, revision int) error {
	// TODO(ericsnow) We need to pass in a base ID...
	res, err := s.resource(entity.URL, resName, revision)
	if err != nil {
		return err
	}
	if err := mongodoc.CheckCharmResource(entity, res); err != nil {
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
