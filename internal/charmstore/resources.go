// Copyright 2016 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package charmstore // import "gopkg.in/juju/charmstore.v5-unstable/internal/charmstore"

import (
	"gopkg.in/errgo.v1"
	"gopkg.in/juju/charm.v6-unstable"
	"gopkg.in/juju/charm.v6-unstable/resource"
	//"gopkg.in/juju/charmrepo.v2-unstable/csclient/params"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	//"gopkg.in/juju/charmstore.v5-unstable/internal/blobstore"
	"gopkg.in/juju/charmstore.v5-unstable/internal/mongodoc"
)

var resourceNotFound = errgo.Newf("resource not found")

// ListResources returns the list of resources for the charm at the
// latest revision for each resource.
func (s Store) ListResources(curl *charm.URL) ([]resource.Resource, error) {
	entity, err := s.FindBestEntity(curl, nil)
	// XXX not found...
	if err != nil {
		return nil, err
	}

	if err := mongodoc.CheckResourceCharm(entity); err != nil {
		return nil, err
	}

	var resources []resource.Resource
	for name, meta := range entity.CharmMeta.Resources {
		res, err := s.latestResource(curl, name)
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
	return resources, nil
}

func (s Store) latestResource(curl *charm.URL, resName string) (resource.Resource, error) {
	var doc mongodoc.LatestResource
	query := bson.D{
		{"charm-url", curl},
		{"name", resName},
	}
	err := s.DB.Resources().Find(query).One(&doc)
	// XXX not found...
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
	// XXX not found...
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
func (s Store) AddResource(curl *charm.URL, res resource.Resource) error {
	// TODO(ericsnow) Validate resource first?

	entity, err := s.FindBestEntity(curl, nil)
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

	// TODO(ericsnow) necessary?
	// Add entity to ElasticSearch.
	if err := s.UpdateSearch(EntityResolvedURL(entity)); err != nil {
		if err := s.DB.Resources().RemoveId(latest.DocID); err != nil {
			logger.Errorf("cannot remove resource after elastic search failure: %v", err)
		}
		if err := s.DB.Resources().RemoveId(doc.DocID); err != nil {
			logger.Errorf("cannot remove resource after elastic search failure: %v", err)
		}
		return errgo.Notef(err, "cannot index %s to ElasticSearch", entity.URL)
	}
	return nil
}
