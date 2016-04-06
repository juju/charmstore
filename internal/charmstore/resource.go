// Copyright 2016 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package charmstore // import "gopkg.in/juju/charmstore.v5-unstable/internal/charmstore"
import (
	"sort"

	"gopkg.in/juju/charm.v6-unstable"
	"gopkg.in/mgo.v2/bson"

	"gopkg.in/juju/charmstore.v5-unstable/internal/mongodoc"
)

// newResourceQuery returns a mongo query doc that will retrieve the
// given named resource and revision associated with the given charm URL.
// If revision is < 0, all revisions of the resource will be selected by
// the query.
func newResourceQuery(url *charm.URL, name string, revision int) bson.D {
	query := make(bson.D, 2, 3)
	query[0] = bson.DocElem{"baseurl", mongodoc.BaseURL(url)}
	query[1] = bson.DocElem{"name", name}
	if revision >= 0 {
		query = append(query, bson.DocElem{"revision", revision})
	}
	return query
}

// sortResources sorts the provided resource docs, The resources are
// sorted first by URL then by name and finally by revision.
func sortResources(resources []*mongodoc.Resource) {
	sort.Sort(resourcesByName(resources))
}

type resourcesByName []*mongodoc.Resource

func (sorted resourcesByName) Len() int      { return len(sorted) }
func (sorted resourcesByName) Swap(i, j int) { sorted[i], sorted[j] = sorted[j], sorted[i] }
func (sorted resourcesByName) Less(i, j int) bool {
	r0, r1 := sorted[i], sorted[j]
	if *r0.BaseURL != *r1.BaseURL {
		return r0.BaseURL.String() < r1.BaseURL.String()
	}
	if r0.Name != r1.Name {
		return r0.Name < r1.Name
	}
	return r0.Revision < r1.Revision
}
