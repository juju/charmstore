// Copyright 2016 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package mongodoc

import (
	"sort"
	"time"

	"gopkg.in/errgo.v1"
	"gopkg.in/juju/charm.v6-unstable"
	"gopkg.in/juju/charm.v6-unstable/resource"
	"gopkg.in/juju/charmrepo.v2-unstable/csclient/params"
	"gopkg.in/mgo.v2/bson"
)

// NewResourceQuery formats the provided details into a mongo query for
// the identified resource.
func NewResourceQuery(cURL *charm.URL, resName string, revision int) bson.D {
	copied := *cURL
	cURL = &copied
	cURL.Revision = -1
	cURL.Series = ""
	query := bson.D{
		{"unresolved-charm-url", cURL},
		{"name", resName},
	}
	if revision >= 0 {
		query = append(query, bson.DocElem{"revision", revision})
	}
	return query
}

// Resource holds the in-database representation of a charm resource
// at a particular revision.
type Resource struct {
	// CharmURL identifies the unresolved charm associated with this
	// resource.
	CharmURL *charm.URL `bson:"unresolved-charm-url"`

	// Name is the name of the resource as defined in the charm
	// metadata.
	Name string `bson:"name"`

	// Revision identifies the specific revision of the resource.
	Revision int `bson:"revision"`

	// Fingerprint is the checksum of the resource file.
	Fingerprint []byte `bson:"fingerprint"`

	// Size is the size of the resource file, in bytes.
	Size int64 `bson:"size"`

	// BlobName holds the name that the resource blob is given in the
	// blob store.
	BlobName string

	// UploadTime is the UTC timestamp from when the resource file was
	// stored in the blob store.
	UploadTime time.Time
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

	if doc.BlobName == "" {
		return errgo.New("missing blob name")
	}

	if doc.UploadTime.IsZero() {
		return errgo.New("missing upload timestamp")
	}

	return nil
}

// SortResources sorts the provided resource docs.
func SortResources(resources []*Resource) {
	sort.Sort(resourcesByName(resources))
}

type resourcesByName []*Resource

func (sorted resourcesByName) Len() int      { return len(sorted) }
func (sorted resourcesByName) Swap(i, j int) { sorted[i], sorted[j] = sorted[j], sorted[i] }
func (sorted resourcesByName) Less(i, j int) bool {
	if sorted[i].Name < sorted[j].Name {
		return true
	}
	if sorted[i].Name == sorted[j].Name && sorted[i].Revision < sorted[j].Revision {
		return true
	}
	return false
}

// NewResourcesQuery formats the provided details into a mongo query
// for the resource revisions associated with the identified *resolved*
// charm URL (in the given channel).
func NewResourcesQuery(cURL *charm.URL, channel params.Channel) bson.D {
	query := bson.D{
		{"resolved-charm-url", cURL},
	}
	if channel != params.NoChannel {
		query = append(query, bson.DocElem{"channel", channel})
	}
	return query
}

// Resources identifies the set of resource revisions for a resolved
// charm, relative to a specific channel,
type Resources struct {
	// CharmURL is the resolved charm ID with which these particular
	// resource revisions were published.
	CharmURL *charm.URL `bson:"resolved-charm-url"`

	// Channel is the channel to which the charm was published with
	// these particular resource revisions.
	Channel params.Channel `bson:"channel"`

	// Revisions maps the charm's resources, by name, to the resource
	// revisions tied to the resolved charm ID.
	Revisions map[string]int `bson:"resource-revisions,omitempty"`
}

// Validate ensures that the doc is valid.
func (doc Resources) Validate() error {
	if doc.Channel == params.NoChannel {
		return errgo.New("missing channel")
	}

	if doc.CharmURL == nil {
		return errgo.New("missing charm URL")
	}
	if doc.CharmURL.Revision == -1 {
		return errgo.New("unresolved charm URLs not supported")
	}
	if doc.CharmURL.Series == "" {
		return errgo.New("series missing")
	}

	for name, revision := range doc.Revisions {
		if name == "" {
			return errgo.New("missing resource name")
		}
		if revision < 0 {
			return errgo.Newf("got negative revision %d for resource %q", revision, name)
		}
	}

	return nil
}
