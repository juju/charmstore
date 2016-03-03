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
	"gopkg.in/juju/charmstore.v5-unstable/internal/router"
)

var resourceNotFound = errgo.Newf("resource not found")

// ListResources returns the list of resources for the charm at the
// latest revision for each resource.
func (s Store) ListResources(entity *mongodoc.Entity) ([]resource.Resource, error) {
	if err := mongodoc.CheckResourceCharm(entity); err != nil {
		return nil, err
	}

	var resources []resource.Resource
	for name, meta := range entity.CharmMeta.Resources {
		res, err := s.latestResource(entity.URL, name)
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

func (s Store) latestResource(curl *charm.URL, resName string) (resource.Resource, error) {
	var doc mongodoc.LatestResource
	query := bson.D{
		{"charm-url", curl},
		{"name", resName},
	}
	err := s.DB.Resources().Find(query).One(&doc)
	if err == mgo.ErrNotFound {
		// TODO(ericsnow) Fail if the resource otherwise exists?
		err = resourceNotFound
	}
	if err != nil {
		return resource.Resource{}, err
	}
	return s.resource(curl, resName, doc.Revision)
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

// TODO(ericsnow) Also store blob in AddResource()?

// AddResource adds the resource to the store and associates it with
// the charm's revision.
func (s Store) AddResource(url *router.ResolvedURL, res resource.Resource) error {
	// TODO(ericsnow) Validate resource first?

	entity, err := s.FindEntity(url, nil)
	if err != nil {
		return err
	}
	return s.insertResource(entity, res)
}

func (s Store) insertResource(entity *mongodoc.Entity, res resource.Resource) error {
	latest, err := mongodoc.NewLatestResource(entity, res.Name, res.Revision)
	if err != nil {
		return err
	}

	doc, err := mongodoc.Resource2Doc(entity.URL, res)
	if err != nil {
		return err
	}
	err = s.DB.Resources().Insert(doc)
	if err != nil && !mgo.IsDup(err) {
		return errgo.Notef(err, "cannot insert resource")
	}

	// TODO(ericsnow) Upsert?
	err = s.DB.Resources().Insert(latest)
	if err != nil {
		if err := s.DB.Resources().RemoveId(doc.DocID); err != nil {
			logger.Errorf("cannot remove resource after elastic search failure: %v", err)
		}
		return errgo.Notef(err, "cannot insert entity")
	}

	// TODO(ericsnow) Add resource to ElasticSearch?

	return nil
}
