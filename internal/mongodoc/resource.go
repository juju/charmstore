// Copyright 2016 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package mongodoc // import "gopkg.in/juju/charmstore.v5-unstable/internal/mongodoc"

import (
	"gopkg.in/juju/charm.v6-unstable"
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
