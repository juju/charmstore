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

// Validate ensures that the doc is valid.
func (doc Resource) Validate() error {
	if doc.CharmURL == nil {
		return errgo.New("missing charm URL")
	}
	if doc.CharmURL.Revision >= 0 {
		return errgo.Newf("resolved charm URLs not supported (got revision %d)", doc.CharmURL.Revision)
	}
	if doc.CharmURL.Series != "" {
		return errgo.Newf("series should not be set (got %q)", doc.CharmURL.Series)
	}
	if doc.Origin != resource.OriginStore.String() {
		return errgo.Newf("unexpected origin %q", doc.Origin)
	}
	if len(doc.Fingerprint) == 0 {
		return errgo.New("missing fingerprint")
	}

	_, err := doc2Resource(doc)
	if err != nil {
		return errgo.Mask(err)
	}
	return nil
}

// doc2Resource returns the resource.Resource represented by the doc.
func doc2Resource(doc Resource) (resource.Resource, error) {
	var res resource.Resource

	resType, err := resource.ParseType(doc.Type)
	if err != nil {
		return res, errgo.Mask(err)
	}

	origin, err := resource.ParseOrigin(doc.Origin)
	if err != nil {
		return res, errgo.Mask(err)
	}

	fp, err := resource.NewFingerprint(doc.Fingerprint)
	if err != nil {
		return res, errgo.Mask(err)
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
		return res, errgo.Mask(err)
	}
	return res, nil
}
