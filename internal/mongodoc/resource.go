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
	CharmURL    *charm.URL `bson:"charm-url"`
	Name        string     `bson:"name"`
	Revision    int        `bson:"revision"`
	Fingerprint []byte     `bson:"fingerprint"`
	Size        int64      `bson:"size"`
}

// Validate ensures that the doc is valid.
func (doc Resource) Validate() error {
	if doc.CharmURL == nil {
		return errgo.New("missing charm URL")
	}
	if doc.CharmURL.Revision != -1 {
		return errgo.Newf("resolved charm URLs not supported (got revision %d)", doc.CharmURL.Revision)
	}
	if doc.CharmURL.Series != "" {
		return errgo.Newf("series should not be set (got %q)", doc.CharmURL.Series)
	}

	if doc.Name == "" {
		return errgo.New("missing name")
	}
	if doc.Revision < 0 {
		return errgo.Newf("got negative revision %d", doc.Revision)
	}

	if len(doc.Fingerprint) == 0 {
		return errgo.New("missing fingerprint")
	}
	fp, err := resource.NewFingerprint(doc.Fingerprint)
	if err != nil {
		return errgo.Notef(err, "bad fingerprint")
	}
	if err := fp.Validate(); err != nil {
		return errgo.Notef(err, "bad fingerprint")
	}

	if doc.Size < 0 {
		return errgo.Newf("got negative size %d", doc.Size)
	}

	return nil
}
