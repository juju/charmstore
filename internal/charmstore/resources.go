// Copyright 2016 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package charmstore // import "gopkg.in/juju/charmstore.v5-unstable/internal/charmstore"
import (
	"fmt"
	"io"
	"sort"
	"time"

	"gopkg.in/errgo.v1"
	"gopkg.in/juju/charm.v6-unstable"
	"gopkg.in/juju/charmrepo.v2-unstable/csclient/params"
	"gopkg.in/mgo.v2"
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

var resourceNotFound = errgo.Newf("resource not found")

// ListResources returns the set of resources for the charm. If the
// unpublished channel is specified then set is composed of the latest
// revision for each resource. Otherwise it holds the revisions declared
// when the charm/channel pair was published.
func (s *Store) ListResources(entity *mongodoc.Entity, channel params.Channel) ([]*mongodoc.Resource, error) {
	if entity.URL.Series == "bundle" {
		return nil, nil
	}
	if entity.CharmMeta == nil {
		return nil, errgo.Newf("entity missing charm metadata")
	}
	baseEntity, err := s.FindBaseEntity(entity.URL, FieldSelector("channelresources"))
	if err != nil {
		return nil, errgo.Mask(err)
	}
	// get all of the resources associated with the charm first.
	resources, revisions, err := s.charmResources(entity.BaseURL)
	if err != nil {
		return nil, errgo.Mask(err)
	}
	if channel != params.UnpublishedChannel {
		revisions = mapRevisions(baseEntity.ChannelResources[channel])
	}
	var docs []*mongodoc.Resource
	for name := range entity.CharmMeta.Resources {
		revision, ok := revisions[name]
		if !ok {
			// There is no matching resource available, perhaps this is an error?
			continue
		}
		doc := resources[name][revision]
		if doc == nil {
			return nil, errgo.Newf("published resource %q, revision %d not found", name, revision)
		}
		docs = append(docs, doc)
	}
	sortResources(docs)
	return docs, nil
}

// charmResources returns all of the currently stored resources for a charm.
func (s *Store) charmResources(baseURL *charm.URL) (map[string]map[int]*mongodoc.Resource, map[string]int, error) {
	resources := make(map[string]map[int]*mongodoc.Resource)
	latest := make(map[string]int)
	iter := s.DB.Resources().Find(bson.D{{"baseurl", baseURL}}).Iter()
	var r mongodoc.Resource
	for iter.Next(&r) {
		resource := r
		if _, ok := resources[r.Name]; !ok {
			resources[r.Name] = make(map[int]*mongodoc.Resource)
		}
		resources[r.Name][r.Revision] = &resource
		if r.Revision >= latest[r.Name] {
			latest[r.Name] = r.Revision
		}
	}
	if err := iter.Close(); err != nil {
		return nil, nil, errgo.Mask(err)
	}
	return resources, latest, nil
}

// mapRevisions converts a list of ResourceRevisions into a map of
// resource name and revision.
func mapRevisions(resourceRevisions []mongodoc.ResourceRevision) map[string]int {
	revisions := make(map[string]int)
	for _, rr := range resourceRevisions {
		revisions[rr.Name] = rr.Revision
	}
	return revisions
}

// UploadResource add blob to the blob store and adds a new resource with
// the given name to the given entity. The revision of the new resource
// will be calculated to be one higher than any existing resources.
func (s *Store) UploadResource(entity *mongodoc.Entity, name string, blob io.Reader, blobHash string, size int64) (*mongodoc.Resource, error) {
	if !charmHasResource(entity.CharmMeta, name) {
		return nil, errgo.Newf("charm does not have resource %q", name)
	}
	blobName, _, err := s.putArchive(blob, size, blobHash)
	if err != nil {
		return nil, errgo.Mask(err)
	}
	res, err := s.addResource(&mongodoc.Resource{
		BaseURL:    entity.BaseURL,
		Name:       name,
		Revision:   -1,
		BlobHash:   blobHash,
		Size:       size,
		BlobName:   blobName,
		UploadTime: time.Now().UTC(),
	})
	if err != nil {
		if err := s.BlobStore.Remove(blobName); err != nil {
			logger.Errorf("cannot remove blob %s after error: %v", blobName, err)
		}
		return nil, errgo.Mask(err)
	}
	return res, nil
}

// addResource adds r to the resources collection. If r does not speify
// a revision number will be one higher than any existing revisions. The
// inserted resource is returned on success.
func (s *Store) addResource(r *mongodoc.Resource) (*mongodoc.Resource, error) {
	if r.Revision < 0 {
		resource := *r
		var err error
		resource.Revision, err = s.nextResourceRevision(r.BaseURL, r.Name)
		if err != nil {
			return nil, errgo.Mask(err)
		}
		r = &resource
	}
	if err := r.Validate(); err != nil {
		return nil, errgo.Mask(err)
	}
	if err := s.DB.Resources().Insert(r); err != nil {
		if mgo.IsDup(err) {
			return nil, errgo.WithCausef(nil, params.ErrDuplicateUpload, "")
		}
		return nil, errgo.Notef(err, "cannot insert resource")
	}
	return r, nil
}

// nextRevisionNumber calculates the next revision number to use for a
// resource.
func (s *Store) nextResourceRevision(baseURL *charm.URL, name string) (int, error) {
	var r mongodoc.Resource
	if err := s.DB.Resources().Find(newResourceQuery(baseURL, name, -1)).Sort("-revision").One(&r); err != nil {
		if err == mgo.ErrNotFound {
			return 0, nil
		}
		return -1, err
	}
	return r.Revision + 1, nil
}

// publishResources publishes the specified set of resources to the
// specified channel for the specified charm.
func (s *Store) publishResources(entity *mongodoc.Entity, channel params.Channel, resources []mongodoc.ResourceRevision) error {
	if channel == params.NoChannel {
		return errgo.New("missing channel")
	}
	if channel == params.UnpublishedChannel {
		return errgo.New("cannot publish to unpublished channel")
	}

	for _, res := range resources {
		if !charmHasResource(entity.CharmMeta, res.Name) {
			return errgo.Newf("charm does not have resource %q", res.Name)
		}
		// TODO(mhilton) find a way to check that the resources exist without fetching each one.
		_, err := s.resource(entity.BaseURL, res.Name, res.Revision)
		if err != nil {
			return errgo.Mask(err, errgo.Is(resourceNotFound))
		}
	}
	channelresources := fmt.Sprintf("channelresources.%s", channel)
	if err := s.DB.BaseEntities().UpdateId(entity.BaseURL, bson.D{{"$set", bson.D{{channelresources, resources}}}}); err != nil {
		return errgo.Mask(err)
	}
	return nil
}

// resource returns the resource with the given URL, name and revision.
// If the resource was not found, it returns an error with a resourceNotFound
// cause.
func (s *Store) resource(url *charm.URL, name string, revision int) (*mongodoc.Resource, error) {
	query := newResourceQuery(url, name, revision)
	var doc mongodoc.Resource
	err := s.DB.Resources().Find(query).One(&doc)
	if err == mgo.ErrNotFound {
		err = resourceNotFound
		return nil, errgo.WithCausef(err, err, "")
	}
	if err != nil {
		return nil, errgo.Mask(err)
	}
	if err := doc.Validate(); err != nil {
		return nil, errgo.Notef(err, "got bad data from DB")
	}
	return &doc, nil
}

func charmHasResource(meta *charm.Meta, name string) bool {
	if meta == nil {
		return false
	}
	_, ok := meta.Resources[name]
	return ok
}
