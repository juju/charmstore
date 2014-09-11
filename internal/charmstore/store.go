// Copyright 2014 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package charmstore

import (
	"crypto/sha256"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/juju/errgo"
	"gopkg.in/juju/charm.v3"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/juju/charmstore/internal/blobstore"
	"github.com/juju/charmstore/internal/mongodoc"
	"github.com/juju/charmstore/params"
)

// Store represents the underlying charm and blob data stores.
type Store struct {
	DB        StoreDatabase
	BlobStore *blobstore.Store

	// Cache for statistics key words (two generations).
	cacheMu       sync.RWMutex
	statsIdNew    map[string]int
	statsIdOld    map[string]int
	statsTokenNew map[int]string
	statsTokenOld map[int]string
}

// NewStore returns a Store that uses the given database.
func NewStore(db *mgo.Database) (*Store, error) {
	s := &Store{
		DB:        StoreDatabase{db},
		BlobStore: blobstore.New(db, "entitystore"),
	}
	if err := s.ensureIndexes(); err != nil {
		return nil, errgo.Notef(err, "cannot ensure indexes")
	}
	return s, nil
}

func (s *Store) ensureIndexes() error {
	indexes := []struct {
		c *mgo.Collection
		i mgo.Index
	}{{
		s.DB.StatCounters(),
		mgo.Index{Key: []string{"k", "t"}, Unique: true},
	}, {
		s.DB.StatTokens(),
		mgo.Index{Key: []string{"t"}, Unique: true},
	}, {
		s.DB.Entities(),
		mgo.Index{Key: []string{"baseurl"}},
	}}
	for _, idx := range indexes {
		err := idx.c.EnsureIndex(idx.i)
		if err != nil {
			return errgo.Mask(err)
		}
	}
	return nil
}

func (s *Store) putArchive(archive blobstore.ReadSeekCloser) (blobName, blobHash, blobHash256 string, size int64, err error) {
	hash := blobstore.NewHash()
	size, err = io.Copy(hash, archive)
	if err != nil {
		return "", "", "", 0, errgo.Mask(err)
	}
	if _, err = archive.Seek(0, 0); err != nil {
		return "", "", "", 0, errgo.Mask(err)
	}
	blobHash = fmt.Sprintf("%x", hash.Sum(nil))
	blobName = bson.NewObjectId().Hex()
	hash256 := sha256.New()
	reader := io.TeeReader(archive, hash256)
	if err = s.BlobStore.PutUnchallenged(reader, blobName, size, blobHash); err != nil {
		return "", "", "", 0, errgo.Mask(err)
	}
	return blobName, blobHash, fmt.Sprintf("%x", hash256.Sum(nil)), size, nil
}

// AddCharmWithArchive is like AddCharm but
// also adds the charm archive to the blob store.
// This method is provided principally so that
// tests can easily create content in the store.
func (s *Store) AddCharmWithArchive(url *charm.Reference, ch charm.Charm) error {
	blobName, blobHash, blobHash256, size, err := s.uploadCharmOrBundle(ch)
	if err != nil {
		return errgo.Mask(err)
	}
	return s.AddCharm(url, ch, blobName, blobHash, blobHash256, size)
}

// AddBundleWithArchive is like AddBundle but
// also adds the charm archive to the blob store.
// This method is provided principally so that
// tests can easily create content in the store.
func (s *Store) AddBundleWithArchive(url *charm.Reference, b charm.Bundle) error {
	blobName, blobHash, blobHash256, size, err := s.uploadCharmOrBundle(b)
	if err != nil {
		return errgo.Mask(err)
	}
	return s.AddBundle(url, b, blobName, blobHash, blobHash256, size)
}

func (s *Store) uploadCharmOrBundle(c interface{}) (blobName, blobHash, blobHash256 string, size int64, err error) {
	archive, err := getArchive(c)
	if err != nil {
		return "", "", "", 0, errgo.Mask(err)
	}
	defer archive.Close()
	return s.putArchive(archive)
}

// AddCharm adds a charm to the blob store and to the entities collection
// associated with the given URL.
func (s *Store) AddCharm(url *charm.Reference, c charm.Charm, blobName, blobHash, blobHash256 string, blobSize int64) error {
	// Add charm metadata to the entities collection.
	err := s.DB.Entities().Insert(&mongodoc.Entity{
		URL:                     url,
		BaseURL:                 baseURL(url),
		BlobHash:                blobHash,
		BlobHash256:             blobHash256,
		BlobName:                blobName,
		Size:                    blobSize,
		UploadTime:              time.Now(),
		CharmMeta:               c.Meta(),
		CharmConfig:             c.Config(),
		CharmActions:            c.Actions(),
		CharmProvidedInterfaces: interfacesForRelations(c.Meta().Provides),
		CharmRequiredInterfaces: interfacesForRelations(c.Meta().Requires),
	})
	if mgo.IsDup(err) {
		return params.ErrDuplicateUpload
	}
	return errgo.Mask(err)
}

// ExpandURL returns all the URLs that the given URL may refer to.
func (s *Store) ExpandURL(url *charm.Reference) ([]*charm.Reference, error) {
	var docs []mongodoc.Entity
	err := s.DB.Entities().Find(bson.D{{
		"baseurl", baseURL(url),
	}}).Select(bson.D{{"_id", 1}}).All(&docs)
	if err != nil {
		return nil, errgo.Mask(err)
	}
	urls := make([]*charm.Reference, 0, len(docs))
	for _, doc := range docs {
		if matchURL((*charm.Reference)(doc.URL), url) {
			urls = append(urls, (*charm.Reference)(doc.URL))
		}
	}
	return urls, nil
}

func matchURL(url, pattern *charm.Reference) bool {
	if pattern.Series != "" && url.Series != pattern.Series {
		return false
	}
	if pattern.Revision != -1 && url.Revision != pattern.Revision {
		return false
	}
	// Check the name for completness only - the
	// query should only be returning URLs with
	// matching names.
	return url.Name == pattern.Name
}

func interfacesForRelations(rels map[string]charm.Relation) []string {
	// Eliminate duplicates by storing interface names into a map.
	interfaces := make(map[string]bool)
	for _, rel := range rels {
		interfaces[rel.Interface] = true
	}
	result := make([]string, 0, len(interfaces))
	for iface := range interfaces {
		result = append(result, iface)
	}
	return result
}

func baseURL(url *charm.Reference) *charm.Reference {
	newURL := *url
	newURL.Revision = -1
	newURL.Series = ""
	return &newURL
}

var errNotImplemented = errgo.Newf("not implemented")

// AddBundle adds a bundle to the blob store and to the entities collection
// associated with the given URL.
func (s *Store) AddBundle(url *charm.Reference, b charm.Bundle, blobName, blobHash, blobHash256 string, blobSize int64) error {
	bundleData := b.Data()
	urls, err := bundleCharms(bundleData)
	if err != nil {
		return errgo.Mask(err)
	}
	err = s.DB.Entities().Insert(&mongodoc.Entity{
		URL:                url,
		BaseURL:            baseURL(url),
		BlobHash:           blobHash,
		BlobHash256:        blobHash256,
		BlobName:           blobName,
		Size:               blobSize,
		UploadTime:         time.Now(),
		BundleData:         bundleData,
		BundleUnitCount:    newInt(bundleUnitCount(bundleData)),
		BundleMachineCount: newInt(bundleMachineCount(bundleData)),
		BundleReadMe:       b.ReadMe(),
		BundleCharms:       urls,
	})
	if mgo.IsDup(err) {
		return params.ErrDuplicateUpload
	}
	return errgo.Mask(err)
}

func newInt(x int) *int {
	return &x
}

// bundleUnitCount returns the number of units created by the bundle.
func bundleUnitCount(b *charm.BundleData) int {
	count := 0
	for _, service := range b.Services {
		count += service.NumUnits
	}
	return count
}

// bundleMachineCount returns the number of machines
// that will be created or used by the bundle.
func bundleMachineCount(b *charm.BundleData) int {
	count := len(b.Machines)
	for _, service := range b.Services {
		// The default placement is "new".
		placement := &charm.UnitPlacement{
			Machine: "new",
		}
		// Check for "new" placements, which means a new machine
		// must be added.
		for _, location := range service.To {
			var err error
			placement, err = charm.ParsePlacement(location)
			if err != nil {
				// Ignore invalid placements - a bundle should always
				// be verified before adding to the charm store so this
				// should never happen in practice.
				continue
			}
			if placement.Machine == "new" {
				count++
			}
		}
		// If there are less elements in To than NumUnits, the last placement
		// element is replicated. For this reason, if the last element is
		// "new", we need to add more machines.
		if placement != nil && placement.Machine == "new" {
			count += service.NumUnits - len(service.To)
		}
	}
	return count
}

// bundleCharms returns all the charm URLs used by a bundle,
// without duplicates.
func bundleCharms(data *charm.BundleData) ([]*charm.Reference, error) {
	// Use a map to de-duplicate the URL list: a bundle can include services
	// deployed by the same charm.
	urlMap := make(map[string]*charm.Reference)
	for _, service := range data.Services {
		url, err := charm.ParseReference(service.Charm)
		if err != nil {
			return nil, errgo.Mask(err)
		}
		urlMap[url.String()] = url
		// Also add the corresponding base URL.
		base := baseURL(url)
		urlMap[base.String()] = base
	}
	urls := make([]*charm.Reference, 0, len(urlMap))
	for _, url := range urlMap {
		urls = append(urls, url)
	}
	return urls, nil
}

// StoreDatabase wraps an mgo.DB ands adds a few convenience methods.
type StoreDatabase struct {
	*mgo.Database
}

// Copy copies the StoreDatabase and its underlying mgo session.
func (s StoreDatabase) Copy() StoreDatabase {
	return StoreDatabase{
		&mgo.Database{
			Name:    s.Name,
			Session: s.Session.Copy(),
		},
	}
}

// Close closes the store database's underlying session.
func (s StoreDatabase) Close() {
	s.Session.Close()
}

// Entities returns the mongo collection where entities are stored.
func (s StoreDatabase) Entities() *mgo.Collection {
	return s.C("entities")
}
