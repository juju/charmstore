// Copyright 2014 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package storetesting // import "gopkg.in/juju/charmstore.v5/internal/storetesting"

import (
	"archive/zip"
	"bytes"
	"fmt"
	"io"
	"strings"

	"gopkg.in/errgo.v1"
	"gopkg.in/juju/charm.v6"
	"gopkg.in/juju/charm.v6/resource"
	"gopkg.in/juju/charmrepo.v3/testing"
	"gopkg.in/yaml.v2"

	"gopkg.in/juju/charmstore.v5/internal/blobstore"
)

// Charms holds the testing charm repository.
var Charms = testing.NewRepo("charm-repo", "quantal")

var _ charm.Bundle = (*Bundle)(nil)

// Bundle implements an in-memory charm.Bundle
// that can be archived.
//
// Note that because it implements charmstore.ArchiverTo,
// it can be used as an argument to charmstore.Store.AddBundleWithArchive.
type Bundle struct {
	*Blob
	data   *charm.BundleData
	readMe string
}

// Data implements charm.Bundle.Data.
func (b *Bundle) Data() *charm.BundleData {
	return b.data
}

// ReadMe implements charm.Bundle.ReadMe.
func (b *Bundle) ReadMe() string {
	return b.readMe
}

// NewBundle returns a bundle implementation
// that contains the given bundle data.
func NewBundle(data *charm.BundleData) *Bundle {
	dataYAML, err := yaml.Marshal(data)
	if err != nil {
		panic(err)
	}
	readMe := "boring"
	return &Bundle{
		data:   data,
		readMe: readMe,
		Blob: NewBlob([]File{{
			Name: "bundle.yaml",
			Data: dataYAML,
		}, {
			Name: "README.md",
			Data: []byte(readMe),
		}}),
	}
}

// Charm implements an in-memory charm.Charm that
// can be archived.
//
// Note that because it implements charmstore.ArchiverTo,
// it can be used as an argument to charmstore.Store.AddCharmWithArchive.
type Charm struct {
	blob    *Blob
	meta    *charm.Meta
	metrics *charm.Metrics
}

var _ charm.Charm = (*Charm)(nil)

// NewCharm returns a charm implementation that contains the given charm
// metadata. All charm.Charm methods other than Meta will return empty
// values. If meta is nil, new(charm.Meta) will be used.
func NewCharm(meta *charm.Meta) *Charm {
	if meta == nil {
		meta = new(charm.Meta)
	}
	return &Charm{
		meta: meta,
	}
}

func (c *Charm) initBlob() {
	if c.blob != nil {
		return
	}
	metaYAML, err := yaml.Marshal(c.meta)
	if err != nil {
		panic(err)
	}
	files := []File{{
		Name: "metadata.yaml",
		Data: metaYAML,
	}, {
		Name: "README.md",
		Data: []byte("boring"),
	}}
	if c.metrics != nil {
		metricsYAML, err := yaml.Marshal(c.metrics)
		if err != nil {
			panic(err)
		}
		files = append(files, File{
			Name: "metrics.yaml",
			Data: metricsYAML,
		})
	}
	c.blob = NewBlob(files)
}

func (c *Charm) WithMetrics(metrics *charm.Metrics) *Charm {
	c.metrics = metrics
	return c
}

// Meta implements charm.Charm.Meta.
func (c *Charm) Meta() *charm.Meta {
	return c.meta
}

// Config implements charm.Charm.Config.
func (c *Charm) Config() *charm.Config {
	return charm.NewConfig()
}

// Metrics implements charm.Charm.Metrics.
func (c *Charm) Metrics() *charm.Metrics {
	return c.metrics
}

// Actions implements charm.Charm.Actions.
func (c *Charm) Actions() *charm.Actions {
	return charm.NewActions()
}

// Revision implements charm.Charm.Revision.
func (c *Charm) Revision() int {
	return 0
}

// ArchiveTo implements charmstore.ArchiverTo.
func (c *Charm) ArchiveTo(w io.Writer) error {
	c.initBlob()
	return c.blob.ArchiveTo(w)
}

// Bytes returns the contents of the charm's archive.
func (c *Charm) Bytes() []byte {
	c.initBlob()
	return c.blob.Bytes()
}

// Size returns the size of the charm's archive blob.
func (c *Charm) Size() int64 {
	c.initBlob()
	return c.blob.Size()
}

// File represents a file which will be added to a new blob.
type File struct {
	Name string
	Data []byte
}

// NewBlob returns a blob that holds the given files.
func NewBlob(files []File) *Blob {
	var blob bytes.Buffer
	zw := zip.NewWriter(&blob)
	for _, f := range files {
		w, err := zw.Create(f.Name)
		if err != nil {
			panic(err)
		}
		if _, err := w.Write(f.Data); err != nil {
			panic(err)
		}
	}
	if err := zw.Close(); err != nil {
		panic(err)
	}
	h := blobstore.NewHash()
	h.Write(blob.Bytes())
	return &Blob{
		data: blob.Bytes(),
		hash: fmt.Sprintf("%x", h.Sum(nil)),
	}
}

// Blob represents a blob of data - a zip archive.
// Since it implements charmstore.ArchiverTo, it
// can be used to add charms or bundles with specific
// contents to the charm store.
type Blob struct {
	data []byte
	hash string
}

// Bytes returns the contents of the blob.
func (b *Blob) Bytes() []byte {
	return b.data
}

// Size returns the size of the blob.
func (b *Blob) Size() int64 {
	return int64(len(b.data))
}

// ArchiveTo implements charmstore.ArchiverTo.ArchiveTo.
func (b *Blob) ArchiveTo(w io.Writer) error {
	_, err := w.Write(b.data)
	return err
}

// MetaWithSupportedSeries returns m with Series
// set to series. If m is nil, new(charm.Meta)
// will be used instead.
func MetaWithSupportedSeries(m *charm.Meta, series ...string) *charm.Meta {
	if m == nil {
		m = new(charm.Meta)
	}
	m.Series = series
	return m
}

// RelationMeta returns charm metadata for a charm
// with the given relations, where each relation
// is specified as a white-space-separated
// triple:
//	role name interface
// where role specifies the role of the interface
// (provides or requires), name holds the relation
// name and interface holds the interface relation type.
func RelationMeta(relations ...string) *charm.Meta {
	provides := make(map[string]charm.Relation)
	requires := make(map[string]charm.Relation)
	for _, rel := range relations {
		r, err := parseRelation(rel)
		if err != nil {
			panic(fmt.Errorf("bad relation %q", err))
		}
		if r.Role == charm.RoleProvider {
			provides[r.Name] = r
		} else {
			requires[r.Name] = r
		}
	}
	return &charm.Meta{
		Provides: provides,
		Requires: requires,
	}
}

func parseRelation(s string) (charm.Relation, error) {
	fields := strings.Fields(s)
	if len(fields) != 3 {
		return charm.Relation{}, errgo.Newf("wrong field count")
	}
	r := charm.Relation{
		Scope:     charm.ScopeGlobal,
		Name:      fields[1],
		Interface: fields[2],
	}
	switch fields[0] {
	case "provides":
		r.Role = charm.RoleProvider
	case "requires":
		r.Role = charm.RoleRequirer
	default:
		return charm.Relation{}, errgo.Newf("unknown role")
	}
	return r, nil
}

// MetaWithResources returns m with Resources set to a set of resources
// with the given names. If m is nil, new(charm.Meta) will be used
// instead.
//
// The path and description of the resources are derived from
// the resource name by adding a "-file" and a " description"
// suffix respectively.
func MetaWithResources(m *charm.Meta, resources ...string) *charm.Meta {
	if m == nil {
		m = new(charm.Meta)
	}
	m.Resources = make(map[string]resource.Meta)
	for _, name := range resources {
		m.Resources[name] = resource.Meta{
			Name:        name,
			Type:        resource.TypeFile,
			Path:        name + "-file",
			Description: name + " description",
		}
	}
	return m
}

// MetaWithCategories returns m with Categories set to categories. If m
// is nil, new(charm.Meta) will be used instead.
func MetaWithCategories(m *charm.Meta, categories ...string) *charm.Meta {
	if m == nil {
		m = new(charm.Meta)
	}
	m.Categories = categories
	return m
}

// MetaWithTags returns m with Tags set to tags. If m is nil,
// new(charm.Meta) will be used instead.
func MetaWithTags(m *charm.Meta, tags ...string) *charm.Meta {
	if m == nil {
		m = new(charm.Meta)
	}
	m.Tags = tags
	return m
}
