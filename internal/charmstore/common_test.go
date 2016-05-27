// Copyright 2016 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package charmstore // import "gopkg.in/juju/charmstore.v5-unstable/internal/charmstore"

import (
	gc "gopkg.in/check.v1"
	"gopkg.in/juju/charm.v6-unstable"
	"gopkg.in/juju/charmrepo.v2-unstable/csclient/params"
	"gopkg.in/macaroon-bakery.v1/bakery"

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

func (s *commonSuite) newStore(c *gc.C, withElasticSearch bool) *Store {
	var si *SearchIndex
	if withElasticSearch {
		si = &SearchIndex{s.ES, s.TestIndex}
	}
	p, err := NewPool(s.Session.DB("juju_test"), si, &bakery.NewServiceParams{}, ServerParams{})
	c.Assert(err, gc.IsNil)
	store := p.Store()
	defer p.Close()
	return store
}

func addRequiredCharms(c *gc.C, store *Store, bundle charm.Bundle) {
	for _, app := range bundle.Data().Applications {
		u := charm.MustParseURL(app.Charm)
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
		err = store.Publish(&rurl, nil, params.StableChannel)
		c.Assert(err, gc.IsNil)
	}
}
