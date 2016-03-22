// Copyright 2016 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package mongodoc // import "gopkg.in/juju/charmstore.v5-unstable/internal/mongodoc"

import (
	"gopkg.in/errgo.v1"
	"gopkg.in/juju/charm.v6-unstable"
	"gopkg.in/juju/charm.v6-unstable/resource"
)

// Resource holds the in-database representation of a charm resource
// at a particular revision.
type Resource struct {
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

	doc := &Resource{
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
