// Copyright 2015 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package mongodoc // import "gopkg.in/juju/charmstore.v5-unstable/internal/mongodoc"

import (
	"fmt"

	"gopkg.in/errgo.v1"
	"gopkg.in/juju/charm.v6-unstable"
	"gopkg.in/juju/charm.v6-unstable/resource"
)

// CheckResourceCharm ensures that the given entity is okay
// to associate with a revisioned resource.
func CheckResourceCharm(entity Entity) error {
	if entity.URL.Series == "bundle" {
		return errgo.Newf("bundles do not have resources")
	}
	if entity.URL.Revision < 0 {
		return errgo.Newf("unrevisioned charms do not have specific resources")
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

// NewLatestResourceID generates the doc ID corresponding to the given info.
func NewLatestResourceID(curl *charm.URL, resName string, revision int) string {
	return fmt.Sprintf("latest-resource#%s#%s#%d", curl, resName, revision)
}

// LatestResource links a revisioned charm to a revisioned resource.
type LatestResource struct {
	DocID    string     `bson:"_id"`
	CharmURL *charm.URL `bson:"charm-url"`
	Resource string     `bson:"name"` // matches Resource
	Revision int        `bson:"revision"`
}

// NewLatestResource packs the provided data into a LatestResource. The
// entity must not be a bundle nor unrevisioned. The charmmetadata must
// have the resource name. The revision must be non-negative.
func NewLatestResource(entity Entity, resName string, revision int) (*LatestResource, error) {
	if err := CheckResourceCharm(entity); err != nil {
		return nil, err
	}
	if !charmHasResource(entity.CharmMeta, resName) {
		return nil, errgo.Newf("charm does not have resource %q", resName)
	}
	if revision < 0 {
		return nil, errgo.Newf("missing resource revision")
	}

	id := NewLatestResourceID(entity.URL, resName, revision)
	latest := &LatestResource{
		DocID:    id,
		CharmURL: entity.URL,
		Resource: resName,
		Revision: revision,
	}
	return latest, nil
}

// NewResourceID generates the doc ID corresponding to the given info.
func NewResourceID(curl *charm.URL, resName string, revision int) string {
	// We ignore the series and revision because resources are specific
	// to the charm rather than to any particular variation of it.
	curl = curl.WithRevision(-1)
	curl.Series = ""
	return fmt.Sprintf("resource#%s#%s#%d", curl, resName, revision)
}

// Resource holds the in-database representation of a charm resource
// at a particular revision.
type Resource struct {
	DocID    string     `bson:"_id"`
	CharmURL *charm.URL `bson:"charm-url"`

	Name        string `bson:"name"`
	Type        string `bson:"type"`
	Path        string `bson:"path"`
	Description string `bson:"comment"`

	Origin      string `bson:"origin"`
	Revision    int    `bson:"revision"`
	Fingerprint []byte `bson:"fingerprint"`
	Size        int64  `bson:"size"`
}

// Resource2Doc converts the resource into a DB doc.
func Resource2Doc(curl *charm.URL, res resource.Resource) (*Resource, error) {
	if curl.Series == "bundle" {
		return nil, errgo.Newf("bundles do not have resources")
	}

	// We ignore the series and revision because resources are specific
	// to the charm rather than to any particular variation of it.
	curl = curl.WithRevision(-1)
	curl.Series = ""

	id := NewResourceID(curl, res.Name, res.Revision)
	doc := &Resource{
		DocID:    id,
		CharmURL: curl,

		Name:        res.Name,
		Type:        res.Type.String(),
		Path:        res.Path,
		Description: res.Description,

		Origin:      res.Origin.String(),
		Revision:    res.Revision,
		Fingerprint: res.Fingerprint.Bytes(),
		Size:        res.Size,
	}
	return doc, nil
}

// Doc2Resource returns the resource.Resource represented by the doc.
func Doc2Resource(doc Resource) (resource.Resource, error) {
	var res resource.Resource

	resType, err := resource.ParseType(doc.Type)
	if err != nil {
		return res, errgo.Notef(err, "got invalid data from DB")
	}

	origin, err := resource.ParseOrigin(doc.Origin)
	if err != nil {
		return res, errgo.Notef(err, "got invalid data from DB")
	}

	fp, err := resource.NewFingerprint(doc.Fingerprint)
	if err != nil {
		return res, errgo.Notef(err, "got invalid data from DB")
	}

	res = resource.Resource{
		Meta: resource.Meta{
			Name:        doc.Name,
			Type:        resType,
			Path:        doc.Path,
			Description: doc.Description,
		},
		Origin:      origin,
		Revision:    doc.Revision,
		Fingerprint: fp,
		Size:        doc.Size,
	}
	if err := res.Validate(); err != nil {
		return res, errgo.Notef(err, "got invalid data from DB")
	}
	return res, nil
}
