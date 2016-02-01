// Copyright 2014 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package v5_test // import "gopkg.in/juju/charmstore.v5-unstable/internal/v5"

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"

	jc "github.com/juju/testing/checkers"
	"github.com/juju/testing/httptesting"
	gc "gopkg.in/check.v1"
	"gopkg.in/juju/charm.v6-unstable"
	"gopkg.in/juju/charmrepo.v2-unstable/csclient/params"
	"gopkg.in/mgo.v2/bson"

	"gopkg.in/juju/charmstore.v5-unstable/internal/blobstore"
	"gopkg.in/juju/charmstore.v5-unstable/internal/charmstore"
	"gopkg.in/juju/charmstore.v5-unstable/internal/mongodoc"
	"gopkg.in/juju/charmstore.v5-unstable/internal/router"
)

// Define fake blob attributes to be used in tests.
var fakeBlobSize, fakeBlobHash = func() (int64, string) {
	b := []byte("fake content")
	h := blobstore.NewHash()
	h.Write(b)
	return int64(len(b)), fmt.Sprintf("%x", h.Sum(nil))
}()

type RelationsSuite struct {
	commonSuite
}

var _ = gc.Suite(&RelationsSuite{})

// metaCharmRelatedCharms defines a bunch of charms to be used in
// the relation tests.
var metaCharmRelatedCharms = map[string]charm.Charm{
	"0 ~charmers/utopic/wordpress-0": &relationTestingCharm{
		provides: map[string]charm.Relation{
			"website": {
				Name:      "website",
				Role:      "provider",
				Interface: "http",
			},
		},
		requires: map[string]charm.Relation{
			"cache": {
				Name:      "cache",
				Role:      "requirer",
				Interface: "memcache",
			},
			"nfs": {
				Name:      "nfs",
				Role:      "requirer",
				Interface: "mount",
			},
		},
	},
	"42 ~charmers/utopic/memcached-42": &relationTestingCharm{
		provides: map[string]charm.Relation{
			"cache": {
				Name:      "cache",
				Role:      "provider",
				Interface: "memcache",
			},
		},
	},
	"1 ~charmers/precise/nfs-1": &relationTestingCharm{
		provides: map[string]charm.Relation{
			"nfs": {
				Name:      "nfs",
				Role:      "provider",
				Interface: "mount",
			},
		},
	},
	"47 ~charmers/trusty/haproxy-47": &relationTestingCharm{
		requires: map[string]charm.Relation{
			"reverseproxy": {
				Name:      "reverseproxy",
				Role:      "requirer",
				Interface: "http",
			},
		},
	},
	"48 ~charmers/precise/haproxy-48": &relationTestingCharm{
		requires: map[string]charm.Relation{
			"reverseproxy": {
				Name:      "reverseproxy",
				Role:      "requirer",
				Interface: "http",
			},
		},
	},
	// development charms should not be included in any results.
	"49 ~charmers/development/precise/haproxy-49": &relationTestingCharm{
		provides: map[string]charm.Relation{
			"reverseproxy": {
				Name:      "reverseproxy",
				Role:      "requirer",
				Interface: "http",
			},
		},
	},
	"1 ~charmers/multi-series-20": &relationTestingCharm{
		supportedSeries: []string{"precise", "trusty", "utopic"},
		requires: map[string]charm.Relation{
			"reverseproxy": {
				Name:      "reverseproxy",
				Role:      "requirer",
				Interface: "http",
			},
		},
	},
}

var metaCharmRelatedTests = []struct {
	// Description of the test.
	about string

	// Charms to be stored in the store before the test is run.
	charms map[string]charm.Charm

	// readACLs holds ACLs for charms that should be given
	// non-public permissions, indexed by URL string
	readACLs map[string][]string

	// The id of the charm for which related charms are returned.
	id string

	// The querystring to append to the resulting charmstore URL.
	querystring string

	// The expected response body.
	expectBody params.RelatedResponse
}{{
	about:  "provides and requires",
	charms: metaCharmRelatedCharms,
	id:     "utopic/wordpress-0",
	expectBody: params.RelatedResponse{
		Provides: map[string][]params.EntityResult{
			"memcache": {{
				Id: charm.MustParseURL("utopic/memcached-42"),
			}},
			"mount": {{
				Id: charm.MustParseURL("precise/nfs-1"),
			}},
		},
		Requires: map[string][]params.EntityResult{
			"http": {{
				Id: charm.MustParseURL("multi-series-1"),
			}, {
				Id: charm.MustParseURL("precise/haproxy-48"),
			}, {
				Id: charm.MustParseURL("trusty/haproxy-47"),
			}},
		},
	},
}, {
	about:  "only provides",
	charms: metaCharmRelatedCharms,
	id:     "trusty/haproxy-47",
	expectBody: params.RelatedResponse{
		Provides: map[string][]params.EntityResult{
			"http": {{
				Id: charm.MustParseURL("utopic/wordpress-0"),
			}},
		},
	},
}, {
	about:  "only requires",
	charms: metaCharmRelatedCharms,
	id:     "utopic/memcached-42",
	expectBody: params.RelatedResponse{
		Requires: map[string][]params.EntityResult{
			"memcache": {{
				Id: charm.MustParseURL("utopic/wordpress-0"),
			}},
		},
	},
}, {
	about: "no relations found",
	charms: map[string]charm.Charm{
		"0 ~charmers/utopic/wordpress-0": &relationTestingCharm{
			provides: map[string]charm.Relation{
				"website": {
					Name:      "website",
					Role:      "provider",
					Interface: "http",
				},
			},
			requires: map[string]charm.Relation{
				"cache": {
					Name:      "cache",
					Role:      "requirer",
					Interface: "memcache",
				},
				"nfs": {
					Name:      "nfs",
					Role:      "requirer",
					Interface: "mount",
				},
			},
		},
	},
	id: "utopic/wordpress-0",
}, {
	about: "no relations defined",
	charms: map[string]charm.Charm{
		"42 ~charmers/utopic/django-42": &relationTestingCharm{},
	},
	id: "utopic/django-42",
}, {
	about: "multiple revisions of the same related charm",
	charms: map[string]charm.Charm{
		"0 ~charmers/trusty/wordpress-0": &relationTestingCharm{
			requires: map[string]charm.Relation{
				"cache": {
					Name:      "cache",
					Role:      "requirer",
					Interface: "memcache",
				},
			},
		},
		"1 ~charmers/utopic/memcached-1": &relationTestingCharm{
			provides: map[string]charm.Relation{
				"cache": {
					Name:      "cache",
					Role:      "provider",
					Interface: "memcache",
				},
			},
		},
		"2 ~charmers/utopic/memcached-2": &relationTestingCharm{
			provides: map[string]charm.Relation{
				"cache": {
					Name:      "cache",
					Role:      "provider",
					Interface: "memcache",
				},
			},
		},
		"3 ~charmers/utopic/memcached-3": &relationTestingCharm{
			provides: map[string]charm.Relation{
				"cache": {
					Name:      "cache",
					Role:      "provider",
					Interface: "memcache",
				},
			},
		},
	},
	id: "trusty/wordpress-0",
	expectBody: params.RelatedResponse{
		Provides: map[string][]params.EntityResult{
			"memcache": {{
				Id: charm.MustParseURL("utopic/memcached-1"),
			}, {
				Id: charm.MustParseURL("utopic/memcached-2"),
			}, {
				Id: charm.MustParseURL("utopic/memcached-3"),
			}},
		},
	},
}, {
	about: "reference ordering",
	charms: map[string]charm.Charm{
		"0 ~charmers/trusty/wordpress-0": &relationTestingCharm{
			requires: map[string]charm.Relation{
				"cache": {
					Name:      "cache",
					Role:      "requirer",
					Interface: "memcache",
				},
				"nfs": {
					Name:      "nfs",
					Role:      "requirer",
					Interface: "mount",
				},
			},
		},
		"1 ~charmers/utopic/memcached-1": &relationTestingCharm{
			provides: map[string]charm.Relation{
				"cache": {
					Name:      "cache",
					Role:      "provider",
					Interface: "memcache",
				},
			},
		},
		"2 ~charmers/utopic/memcached-2": &relationTestingCharm{
			provides: map[string]charm.Relation{
				"cache": {
					Name:      "cache",
					Role:      "provider",
					Interface: "memcache",
				},
			},
		},
		"90 ~charmers/utopic/redis-90": &relationTestingCharm{
			provides: map[string]charm.Relation{
				"cache": {
					Name:      "cache",
					Role:      "provider",
					Interface: "memcache",
				},
			},
		},
		"47 ~charmers/trusty/nfs-47": &relationTestingCharm{
			provides: map[string]charm.Relation{
				"nfs": {
					Name:      "nfs",
					Role:      "provider",
					Interface: "mount",
				},
			},
		},
		"42 ~charmers/precise/nfs-42": &relationTestingCharm{
			provides: map[string]charm.Relation{
				"nfs": {
					Name:      "nfs",
					Role:      "provider",
					Interface: "mount",
				},
			},
		},
		"47 ~charmers/precise/nfs-47": &relationTestingCharm{
			provides: map[string]charm.Relation{
				"nfs": {
					Name:      "nfs",
					Role:      "provider",
					Interface: "mount",
				},
			},
		},
	},
	id: "trusty/wordpress-0",
	expectBody: params.RelatedResponse{
		Provides: map[string][]params.EntityResult{
			"memcache": {{
				Id: charm.MustParseURL("utopic/memcached-1"),
			}, {
				Id: charm.MustParseURL("utopic/memcached-2"),
			}, {
				Id: charm.MustParseURL("utopic/redis-90"),
			}},
			"mount": {{
				Id: charm.MustParseURL("precise/nfs-42"),
			}, {
				Id: charm.MustParseURL("precise/nfs-47"),
			}, {
				Id: charm.MustParseURL("trusty/nfs-47"),
			}},
		},
	},
}, {
	about:       "includes",
	charms:      metaCharmRelatedCharms,
	id:          "precise/nfs-1",
	querystring: "?include=archive-size&include=charm-metadata",
	expectBody: params.RelatedResponse{
		Requires: map[string][]params.EntityResult{
			"mount": {{
				Id: charm.MustParseURL("utopic/wordpress-0"),
				Meta: map[string]interface{}{
					"archive-size": params.ArchiveSizeResponse{Size: fakeBlobSize},
					"charm-metadata": &charm.Meta{
						Provides: map[string]charm.Relation{
							"website": {
								Name:      "website",
								Role:      "provider",
								Interface: "http",
							},
						},
						Requires: map[string]charm.Relation{
							"cache": {
								Name:      "cache",
								Role:      "requirer",
								Interface: "memcache",
							},
							"nfs": {
								Name:      "nfs",
								Role:      "requirer",
								Interface: "mount",
							},
						},
					},
				},
			}},
		},
	},
}, {
	about:  "don't show charms if you don't have perms for 'em",
	charms: metaCharmRelatedCharms,
	readACLs: map[string][]string{
		"~charmers/memcached": []string{"noone"},
	},
	id: "utopic/wordpress-0",
	expectBody: params.RelatedResponse{
		Provides: map[string][]params.EntityResult{
			"mount": {{
				Id: charm.MustParseURL("precise/nfs-1"),
			}},
		},
		Requires: map[string][]params.EntityResult{
			"http": {{
				Id: charm.MustParseURL("multi-series-1"),
			}, {
				Id: charm.MustParseURL("precise/haproxy-48"),
			}, {
				Id: charm.MustParseURL("trusty/haproxy-47"),
			}},
		},
	},
}}

func (s *RelationsSuite) TestMetaCharmRelated(c *gc.C) {
	for i, test := range metaCharmRelatedTests {
		c.Logf("\ntest %d: %s", i, test.about)
		s.addCharms(c, test.charms)
		s.setPerms(c, test.readACLs)
		storeURL := storeURL(test.id + "/meta/charm-related" + test.querystring)
		httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
			Handler:      s.srv,
			URL:          storeURL,
			ExpectStatus: http.StatusOK,
			ExpectBody:   test.expectBody,
		})
		// Clean up the entities in the store.
		_, err := s.store.DB.Entities().RemoveAll(nil)
		c.Assert(err, gc.IsNil)
		_, err = s.store.DB.BaseEntities().RemoveAll(nil)
		c.Assert(err, gc.IsNil)
	}
}

func (s *RelationsSuite) TestMetaCharmRelatedIncludeError(c *gc.C) {
	s.addCharms(c, metaCharmRelatedCharms)
	storeURL := storeURL("utopic/wordpress-0/meta/charm-related?include=no-such")
	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler:      s.srv,
		URL:          storeURL,
		ExpectStatus: http.StatusInternalServerError,
		ExpectBody: params.Error{
			Message: `cannot retrieve the charm requires: unrecognized metadata name "no-such"`,
		},
	})
}

// relationTestingCharm implements charm.Charm, and it is used for testing
// charm relations.
type relationTestingCharm struct {
	supportedSeries []string
	provides        map[string]charm.Relation
	requires        map[string]charm.Relation
}

func (ch *relationTestingCharm) Meta() *charm.Meta {
	// The only metadata we are interested in is the relation data.
	return &charm.Meta{
		Series:   ch.supportedSeries,
		Provides: ch.provides,
		Requires: ch.requires,
	}
}

func (ch *relationTestingCharm) Config() *charm.Config {
	// For the purposes of this implementation, the charm configuration is not
	// relevant.
	return nil
}

func (e *relationTestingCharm) Metrics() *charm.Metrics {
	return nil
}

func (ch *relationTestingCharm) Actions() *charm.Actions {
	// For the purposes of this implementation, the charm actions are not
	// relevant.
	return nil
}

func (ch *relationTestingCharm) Revision() int {
	// For the purposes of this implementation, the charm revision is not
	// relevant.
	return 0
}

// metaBundlesContainingBundles defines a bunch of bundles to be used in
// the bundles-containing tests.
var metaBundlesContainingBundles = map[string]charm.Bundle{
	"0 ~charmers/bundle/wordpress-simple-0": relationTestingBundle([]string{
		"cs:utopic/wordpress-42",
		"cs:utopic/mysql-0",
	}),
	"1 ~charmers/bundle/wordpress-simple-1": relationTestingBundle([]string{
		"cs:utopic/wordpress-47",
		"cs:utopic/mysql-1",
	}),
	"1 ~charmers/bundle/wordpress-complex-1": relationTestingBundle([]string{
		"cs:utopic/wordpress-42",
		"cs:utopic/wordpress-47",
		"cs:trusty/mysql-0",
		"cs:trusty/mysql-1",
		"cs:trusty/memcached-2",
	}),
	"42 ~charmers/bundle/django-generic-42": relationTestingBundle([]string{
		"django",
		"django",
		"mysql-1",
		"trusty/memcached",
	}),
	"0 ~charmers/bundle/useless-0": relationTestingBundle([]string{
		"cs:utopic/wordpress-42",
		"precise/mediawiki-10",
	}),
	"46 ~charmers/bundle/mediawiki-simple-46": relationTestingBundle([]string{
		"precise/mediawiki-0",
	}),
	"47 ~charmers/bundle/mediawiki-simple-47": relationTestingBundle([]string{
		"precise/mediawiki-0",
		"mysql",
	}),
	"48 ~charmers/bundle/mediawiki-simple-48": relationTestingBundle([]string{
		"precise/mediawiki-0",
	}),
	"~bob/bundle/bobthebundle-2": relationTestingBundle([]string{
		"precise/mediawiki-0",
	}),
}

var metaBundlesContainingTests = []struct {
	// Description of the test.
	about string
	// The id of the charm for which related bundles are returned.
	id string
	// The querystring to append to the resulting charmstore URL.
	querystring string
	// The expected status code of the response.
	expectStatus int
	// The expected response body.
	expectBody interface{}
}{{
	about:        "specific charm present in several bundles",
	id:           "utopic/wordpress-42",
	expectStatus: http.StatusOK,
	expectBody: []*params.MetaAnyResponse{{
		Id: charm.MustParseURL("bundle/useless-0"),
	}, {
		Id: charm.MustParseURL("bundle/wordpress-complex-1"),
	}, {
		Id: charm.MustParseURL("bundle/wordpress-simple-0"),
	}},
}, {
	about:        "specific charm present in one bundle",
	id:           "trusty/memcached-2",
	expectStatus: http.StatusOK,
	expectBody: []*params.MetaAnyResponse{{
		Id: charm.MustParseURL("bundle/wordpress-complex-1"),
	}},
}, {
	about:        "specific charm not present in any bundle",
	id:           "trusty/django-42",
	expectStatus: http.StatusOK,
	expectBody:   []*params.MetaAnyResponse{},
}, {
	about:        "specific charm with includes",
	id:           "trusty/mysql-1",
	querystring:  "?include=archive-size&include=bundle-metadata",
	expectStatus: http.StatusOK,
	expectBody: []*params.MetaAnyResponse{{
		Id: charm.MustParseURL("bundle/wordpress-complex-1"),
		Meta: map[string]interface{}{
			"archive-size":    params.ArchiveSizeResponse{Size: fakeBlobSize},
			"bundle-metadata": metaBundlesContainingBundles["1 ~charmers/bundle/wordpress-complex-1"].Data(),
		},
	}},
}, {
	about:        "partial charm id",
	id:           "mysql", // The test will add cs:utopic/mysql-0.
	expectStatus: http.StatusOK,
	expectBody: []*params.MetaAnyResponse{{
		Id: charm.MustParseURL("bundle/wordpress-simple-0"),
	}},
}, {
	about:        "any series set to true",
	id:           "trusty/mysql-0",
	querystring:  "?any-series=1",
	expectStatus: http.StatusOK,
	expectBody: []*params.MetaAnyResponse{{
		Id: charm.MustParseURL("bundle/wordpress-complex-1"),
	}, {
		Id: charm.MustParseURL("bundle/wordpress-simple-0"),
	}},
}, {
	about:        "any series and all-results set to true",
	id:           "trusty/mysql-0",
	querystring:  "?any-series=1&all-results=1",
	expectStatus: http.StatusOK,
	expectBody: []*params.MetaAnyResponse{{
		Id: charm.MustParseURL("bundle/wordpress-complex-1"),
	}, {
		// This result is included even if the latest wordpress-simple does not
		// contain the mysql-0 charm.
		Id: charm.MustParseURL("bundle/wordpress-simple-0"),
	}},
}, {
	about:        "invalid any series",
	id:           "utopic/mysql-0",
	querystring:  "?any-series=true",
	expectStatus: http.StatusBadRequest,
	expectBody: params.Error{
		Code:    params.ErrBadRequest,
		Message: `invalid value for any-series: unexpected bool value "true" (must be "0" or "1")`,
	},
}, {
	about:        "any revision set to true",
	id:           "trusty/memcached-99",
	querystring:  "?any-revision=1",
	expectStatus: http.StatusOK,
	expectBody: []*params.MetaAnyResponse{{
		Id: charm.MustParseURL("bundle/django-generic-42"),
	}, {
		Id: charm.MustParseURL("bundle/wordpress-complex-1"),
	}},
}, {
	about:        "invalid any revision",
	id:           "trusty/memcached-99",
	querystring:  "?any-revision=why-not",
	expectStatus: http.StatusBadRequest,
	expectBody: params.Error{
		Code:    params.ErrBadRequest,
		Message: `invalid value for any-revision: unexpected bool value "why-not" (must be "0" or "1")`,
	},
}, {
	about:        "all-results set to true",
	id:           "precise/mediawiki-0",
	expectStatus: http.StatusOK,
	querystring:  "?all-results=1",
	expectBody: []*params.MetaAnyResponse{{
		Id: charm.MustParseURL("bundle/mediawiki-simple-48"),
	}, {
		Id: charm.MustParseURL("bundle/mediawiki-simple-47"),
	}, {
		Id: charm.MustParseURL("bundle/mediawiki-simple-46"),
	}, {
		Id: charm.MustParseURL("~bob/bundle/bobthebundle-2"),
	}},
}, {
	about:        "all-results set to false",
	id:           "precise/mediawiki-0",
	expectStatus: http.StatusOK,
	expectBody: []*params.MetaAnyResponse{{
		Id: charm.MustParseURL("bundle/mediawiki-simple-48"),
	}, {
		Id: charm.MustParseURL("~bob/bundle/bobthebundle-2"),
	}},
}, {
	about:        "invalid all-results",
	id:           "trusty/memcached-99",
	querystring:  "?all-results=yes!",
	expectStatus: http.StatusBadRequest,
	expectBody: params.Error{
		Code:    params.ErrBadRequest,
		Message: `invalid value for all-results: unexpected bool value "yes!" (must be "0" or "1")`,
	},
}, {
	about:        "any series and revision, all results",
	id:           "saucy/mysql-99",
	querystring:  "?any-series=1&any-revision=1&all-results=1",
	expectStatus: http.StatusOK,
	expectBody: []*params.MetaAnyResponse{{
		Id: charm.MustParseURL("bundle/django-generic-42"),
	}, {
		Id: charm.MustParseURL("bundle/mediawiki-simple-47"),
	}, {
		Id: charm.MustParseURL("bundle/wordpress-complex-1"),
	}, {
		Id: charm.MustParseURL("bundle/wordpress-simple-1"),
	}, {
		Id: charm.MustParseURL("bundle/wordpress-simple-0"),
	}},
}, {
	about:        "any series, any revision",
	id:           "saucy/mysql-99",
	querystring:  "?any-series=1&any-revision=1",
	expectStatus: http.StatusOK,
	expectBody: []*params.MetaAnyResponse{{
		Id: charm.MustParseURL("bundle/django-generic-42"),
	}, {
		Id: charm.MustParseURL("bundle/mediawiki-simple-47"),
	}, {
		Id: charm.MustParseURL("bundle/wordpress-complex-1"),
	}, {
		Id: charm.MustParseURL("bundle/wordpress-simple-1"),
	}},
}, {
	about:        "any series and revision, last results",
	id:           "saucy/mediawiki",
	querystring:  "?any-series=1&any-revision=1",
	expectStatus: http.StatusOK,
	expectBody: []*params.MetaAnyResponse{{
		Id: charm.MustParseURL("bundle/mediawiki-simple-48"),
	}, {
		Id: charm.MustParseURL("bundle/useless-0"),
	}, {
		Id: charm.MustParseURL("~bob/bundle/bobthebundle-2"),
	}},
}, {
	about:        "any series and revision with includes",
	id:           "saucy/wordpress-99",
	querystring:  "?any-series=1&any-revision=1&include=archive-size&include=bundle-metadata",
	expectStatus: http.StatusOK,
	expectBody: []*params.MetaAnyResponse{{
		Id: charm.MustParseURL("bundle/useless-0"),
		Meta: map[string]interface{}{
			"archive-size":    params.ArchiveSizeResponse{Size: fakeBlobSize},
			"bundle-metadata": metaBundlesContainingBundles["0 ~charmers/bundle/useless-0"].Data(),
		},
	}, {
		Id: charm.MustParseURL("bundle/wordpress-complex-1"),
		Meta: map[string]interface{}{
			"archive-size":    params.ArchiveSizeResponse{Size: fakeBlobSize},
			"bundle-metadata": metaBundlesContainingBundles["1 ~charmers/bundle/wordpress-complex-1"].Data(),
		},
	}, {
		Id: charm.MustParseURL("bundle/wordpress-simple-1"),
		Meta: map[string]interface{}{
			"archive-size":    params.ArchiveSizeResponse{Size: fakeBlobSize},
			"bundle-metadata": metaBundlesContainingBundles["1 ~charmers/bundle/wordpress-simple-1"].Data(),
		},
	}},
}, {
	about:        "include-error",
	id:           "utopic/wordpress-42",
	querystring:  "?include=no-such",
	expectStatus: http.StatusInternalServerError,
	expectBody: params.Error{
		Message: `unrecognized metadata name "no-such"`,
	},
}}

func (s *RelationsSuite) TestMetaBundlesContaining(c *gc.C) {
	// Add the bundles used for testing to the database.
	for id, b := range metaBundlesContainingBundles {
		url := mustParseResolvedURL(id)
		// The blob related info are not used in these tests.
		// The charm-bundle relations are retrieved from the entities
		// collection, without accessing the blob store.
		err := s.store.AddBundle(b, charmstore.AddParams{
			URL:      url,
			BlobName: "blobName",
			BlobHash: fakeBlobHash,
			BlobSize: fakeBlobSize,
		})
		c.Assert(err, gc.IsNil)
		err = s.store.SetPerms(&url.URL, "read", params.Everyone, url.URL.User)
		c.Assert(err, gc.IsNil)
	}

	for i, test := range metaBundlesContainingTests {
		c.Logf("test %d: %s", i, test.about)

		// Expand the URL if required before adding the charm to the database,
		// so that at least one matching charm can be resolved.
		rurl := &router.ResolvedURL{
			URL:                 *charm.MustParseURL(test.id),
			PromulgatedRevision: -1,
		}
		if rurl.URL.Series == "" {
			rurl.URL.Series = "utopic"
		}
		if rurl.URL.Revision == -1 {
			rurl.URL.Revision = 0
		}
		if rurl.URL.User == "" {
			rurl.URL.User = "charmers"
			rurl.PromulgatedRevision = rurl.URL.Revision
		}
		// Add the charm we need bundle info on to the database.
		err := s.store.AddCharm(&relationTestingCharm{}, charmstore.AddParams{
			URL:      rurl,
			BlobName: "blobName",
			BlobHash: fakeBlobHash,
			BlobSize: fakeBlobSize,
		})
		c.Assert(err, gc.IsNil)
		err = s.store.SetPerms(&rurl.URL, "read", params.Everyone, rurl.URL.User)
		c.Assert(err, gc.IsNil)
		err = s.store.Publish(rurl, charmstore.StableChannel)
		c.Assert(err, gc.IsNil)

		// Perform the request and ensure the response is what we expect.
		storeURL := storeURL(test.id + "/meta/bundles-containing" + test.querystring)
		httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
			Handler:      s.srv,
			URL:          storeURL,
			ExpectStatus: test.expectStatus,
			ExpectBody:   sameMetaAnyResponses(test.expectBody),
		})

		// Clean up the charm entity and base entity in the store.
		err = s.store.DB.Entities().Remove(bson.D{{"_id", &rurl.URL}})
		c.Assert(err, gc.IsNil)
		err = s.store.DB.BaseEntities().Remove(bson.D{{"_id", mongodoc.BaseURL(&rurl.URL)}})
		c.Assert(err, gc.IsNil)
	}
}

func (s *RelationsSuite) TestMetaBundlesContainingBundleACL(c *gc.C) {
	// Add the bundles used for testing to the database.
	for id, b := range metaBundlesContainingBundles {
		url := mustParseResolvedURL(id)
		// The blob related info are not used in these tests.
		// The charm-bundle relations are retrieved from the entities
		// collection, without accessing the blob store.
		err := s.store.AddBundle(b, charmstore.AddParams{
			URL:      url,
			BlobName: "blobName",
			BlobHash: fakeBlobHash,
			BlobSize: fakeBlobSize,
		})
		c.Assert(err, gc.IsNil)
		if url.URL.Name == "useless" {
			// The useless bundle is not available for "everyone".
			err = s.store.SetPerms(&url.URL, "read", url.URL.User)
			c.Assert(err, gc.IsNil)
			continue
		}
		err = s.store.SetPerms(&url.URL, "read", params.Everyone, url.URL.User)
		c.Assert(err, gc.IsNil)
	}
	rurl := mustParseResolvedURL("42 ~charmers/utopic/wordpress-42")
	// Add the charm we need bundle info on to the database.
	err := s.store.AddCharm(&relationTestingCharm{}, charmstore.AddParams{
		URL:      rurl,
		BlobName: "blobName",
		BlobHash: fakeBlobHash,
		BlobSize: fakeBlobSize,
	})
	c.Assert(err, gc.IsNil)
	err = s.store.SetPerms(&rurl.URL, "read", params.Everyone, rurl.URL.User)
	c.Assert(err, gc.IsNil)

	// Perform the request and ensure that the useless bundle isn't listed.
	storeURL := storeURL("utopic/wordpress-42/meta/bundles-containing")
	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler: s.srv,
		URL:     storeURL,
		ExpectBody: sameMetaAnyResponses([]*params.MetaAnyResponse{{
			Id: charm.MustParseURL("bundle/wordpress-complex-1"),
		}, {
			Id: charm.MustParseURL("bundle/wordpress-simple-0"),
		}}),
	})
}

// sameMetaAnyResponses returns a BodyAsserter that checks whether the meta/any response
// matches the expected one, even if the results appear in a different order.
func sameMetaAnyResponses(expect interface{}) httptesting.BodyAsserter {
	return func(c *gc.C, m json.RawMessage) {
		expectMeta, ok := expect.([]*params.MetaAnyResponse)
		if !ok {
			c.Assert(string(m), jc.JSONEquals, expect)
			return
		}
		var got []*params.MetaAnyResponse
		err := json.Unmarshal(m, &got)
		c.Assert(err, gc.IsNil)
		sort.Sort(metaAnyResponseById(got))
		sort.Sort(metaAnyResponseById(expectMeta))
		data, err := json.Marshal(got)
		c.Assert(err, gc.IsNil)
		c.Assert(string(data), jc.JSONEquals, expect)
	}
}

// relationTestingBundle returns a bundle for use in relation
// testing. The urls parameter holds a list of charm references
// to be included in the bundle.
// For each URL, a corresponding service is automatically created.
func relationTestingBundle(urls []string) charm.Bundle {
	services := make(map[string]*charm.ServiceSpec, len(urls))
	for i, url := range urls {
		service := &charm.ServiceSpec{
			Charm:    url,
			NumUnits: 1,
		}
		services[fmt.Sprintf("service-%d", i)] = service
	}
	return &testingBundle{
		data: &charm.BundleData{
			Services: services,
		},
	}
}

// testingBundle is a bundle implementation that
// returns bundle metadata held in the data field.
type testingBundle struct {
	data *charm.BundleData
}

func (b *testingBundle) Data() *charm.BundleData {
	return b.data
}

func (b *testingBundle) ReadMe() string {
	// For the purposes of this implementation, the charm readme is not
	// relevant.
	return ""
}

type metaAnyResponseById []*params.MetaAnyResponse

func (s metaAnyResponseById) Len() int      { return len(s) }
func (s metaAnyResponseById) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s metaAnyResponseById) Less(i, j int) bool {
	return s[i].Id.String() < s[j].Id.String()
}

// mustParseResolvedURL parses a resolved URL in string form, with
// the optional promulgated revision preceding the entity URL
// separated by a space.
func mustParseResolvedURL(urlStr string) *router.ResolvedURL {
	s := strings.Fields(urlStr)
	promRev := -1
	switch len(s) {
	default:
		panic(fmt.Errorf("invalid resolved URL string %q", urlStr))
	case 2:
		var err error
		promRev, err = strconv.Atoi(s[0])
		if err != nil || promRev < 0 {
			panic(fmt.Errorf("invalid resolved URL string %q", urlStr))
		}
	case 1:
	}
	url := charm.MustParseURL(s[len(s)-1])
	return &router.ResolvedURL{
		URL:                 *url.WithChannel(""),
		PromulgatedRevision: promRev,
		Development:         url.Channel == charm.DevelopmentChannel,
	}
}
