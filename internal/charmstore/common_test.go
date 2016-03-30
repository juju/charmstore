// Copyright 2016 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package charmstore // import "gopkg.in/juju/charmstore.v5-unstable/internal/charmstore"

import (
	jc "github.com/juju/testing/checkers"
	gc "gopkg.in/check.v1"
	"gopkg.in/juju/charm.v6-unstable"
	"gopkg.in/juju/charmrepo.v2-unstable/csclient/params"
	"gopkg.in/macaroon-bakery.v1/bakery"

	"gopkg.in/juju/charmstore.v5-unstable/internal/mongodoc"
	"gopkg.in/juju/charmstore.v5-unstable/internal/router"
	"gopkg.in/juju/charmstore.v5-unstable/internal/storetesting"
)

type commonSuite struct {
	storetesting.IsolatedMgoESSuite
	index string
}

// addRequiredCharms adds any charms required by the given
// bundle that are not already in the store.
func (s *commonSuite) addRequiredCharms(c *gc.C, bundle charm.Bundle) {
	store := s.newStore(c, true)
	defer store.Close()
	addRequiredCharms(c, store, bundle)
}

func (s *commonSuite) newStore(c *gc.C, withES bool) *Store {
	var si *SearchIndex
	if withES {
		si = &SearchIndex{s.ES, s.TestIndex}
	}
	p, err := NewPool(s.Session.DB("juju_test"), si, &bakery.NewServiceParams{}, ServerParams{})
	c.Assert(err, gc.IsNil)
	store := p.Store()
	defer p.Close()
	return store
}

func addCharm(c *gc.C, store *Store, curl *charm.URL) (*mongodoc.Entity, *charm.CharmDir) {
	resolvedURL := MustParseResolvedURL(curl.String())
	ch := storetesting.Charms.CharmDir(curl.Name)
	err := store.AddCharmWithArchive(resolvedURL, ch)
	c.Assert(err, jc.ErrorIsNil)
	entity, err := store.FindEntity(resolvedURL, nil)
	c.Assert(err, jc.ErrorIsNil)
	return entity, ch
}

func addBundle(c *gc.C, store *Store, curl *charm.URL) *mongodoc.Entity {
	resolvedURL := MustParseResolvedURL(curl.String())
	b := storetesting.Charms.BundleDir(curl.Name)
	addRequiredCharms(c, store, b)
	err := store.AddBundleWithArchive(resolvedURL, b)
	c.Assert(err, jc.ErrorIsNil)
	entity, err := store.FindEntity(resolvedURL, nil)
	c.Assert(err, jc.ErrorIsNil)
	return entity
}

func addRequiredCharms(c *gc.C, store *Store, bundle charm.Bundle) {
	for _, svc := range bundle.Data().Services {
		u := charm.MustParseURL(svc.Charm)
		if _, err := store.FindBestEntity(u, params.NoChannel, nil); err == nil {
			continue
		}
		if u.Revision == -1 {
			u.Revision = 0
		}
		var rurl router.ResolvedURL
		rurl.URL = *u
		ch := storetesting.Charms.CharmDir(u.Name)
		if len(ch.Meta().Series) == 0 && u.Series == "" {
			rurl.URL.Series = "trusty"
		}
		if u.User == "" {
			rurl.URL.User = "charmers"
			rurl.PromulgatedRevision = rurl.URL.Revision
		} else {
			rurl.PromulgatedRevision = -1
		}
		err := store.AddCharmWithArchive(&rurl, ch)
		c.Assert(err, gc.IsNil)
		err = store.Publish(&rurl, params.StableChannel)
		c.Assert(err, gc.IsNil)
	}
}
