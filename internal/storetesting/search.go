// Copyright 2014 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package storetesting

import (
	"sort"

	"github.com/juju/charm/v8"
	"github.com/juju/charmrepo/v6/csclient/params"

	"gopkg.in/juju/charmstore.v5/internal/mongodoc"
	"gopkg.in/juju/charmstore.v5/internal/router"
	"gopkg.in/juju/charmstore.v5/internal/series"
)

// SearchSeries contains the list of charm series that will be indexed in
// the search in descending order of preference.
var SearchSeries = func() []string {
	var ss []string
	for k, v := range series.Series {
		if v.Distribution != series.Ubuntu || !v.SearchIndex {
			continue
		}
		ss = append(ss, k)
	}
	sort.Slice(ss, func(i, j int) bool {
		return series.Series[ss[i]].SearchBoost > series.Series[ss[j]].SearchBoost
	})
	return ss
}()

// A SearchEntity is an entity that is used in search tests.
type SearchEntity struct {
	URL                 *charm.URL
	PromulgatedRevision int
	SupportedSeries     []string
	Charm               *Charm
	Bundle              *Bundle
	ACL                 []string
	Downloads           int
}

func (e SearchEntity) Entity() *mongodoc.Entity {
	var purl *charm.URL
	if e.PromulgatedRevision > -1 {
		purl = new(charm.URL)
		*purl = *e.URL
		purl.User = ""
		purl.Revision = e.PromulgatedRevision
	}
	supportedSeries := e.SupportedSeries
	if e.URL.Series == "bundle" {
		supportedSeries = nil
	} else if e.URL.Series != "" {
		supportedSeries = []string{e.URL.Series}
	}
	return &mongodoc.Entity{
		URL:                 e.URL,
		PromulgatedURL:      purl,
		PromulgatedRevision: e.PromulgatedRevision,
		SupportedSeries:     supportedSeries,
	}
}

func (e SearchEntity) ResolvedURL() *router.ResolvedURL {
	return &router.ResolvedURL{
		URL:                 *e.URL,
		PromulgatedRevision: e.PromulgatedRevision,
	}
}

var SearchEntities = map[string]SearchEntity{
	"wordpress": {
		URL:                 charm.MustParseURL("cs:~charmers/" + SearchSeries[0] + "/wordpress-23"),
		PromulgatedRevision: 23,
		Charm: NewCharm(&charm.Meta{
			Description: "blog",
			Provides: map[string]charm.Relation{
				"url": {
					Name:      "url",
					Interface: "http",
					Scope:     charm.ScopeGlobal,
				},
				"monitoring-port": {
					Name:      "monitoring-port",
					Interface: "monitoring",
					Scope:     charm.ScopeContainer,
				},
			},
			Requires: map[string]charm.Relation{
				"mysql": {
					Name:      "mysql",
					Interface: "mysql",
					Scope:     charm.ScopeGlobal,
				},
				"cache": {
					Name:      "cache",
					Interface: "varnish",
					Scope:     charm.ScopeGlobal,
				},
			},
			Categories: []string{"wordpress", "wordpressCAT"},
			Tags:       []string{"wordpressTAG"},
		}),
		ACL: []string{params.Everyone},
	},
	"mysql": {
		URL:                 charm.MustParseURL("cs:~openstack-charmers/" + SearchSeries[2] + "/mysql-7"),
		PromulgatedRevision: 7,
		Charm: NewCharm(&charm.Meta{
			Summary:     "Database Engine",
			Description: "database",
			Provides: map[string]charm.Relation{
				"mysql": {
					Name:      "mysql",
					Interface: "mysql",
					Scope:     charm.ScopeGlobal,
				},
			},
			Categories: []string{"mysql"},
			Tags:       []string{"mysqlTAG", "bar"},
		}),
		ACL:       []string{params.Everyone},
		Downloads: 3,
	},
	"varnish": {
		URL:                 charm.MustParseURL("cs:~foo/" + SearchSeries[2] + "/varnish-1"),
		PromulgatedRevision: -1,
		Charm: NewCharm(&charm.Meta{
			Summary:     "Database Engine",
			Description: "database",
			Categories:  []string{"varnish"},
			Tags:        []string{"varnishTAG"},
		}),
		ACL:       []string{params.Everyone},
		Downloads: 5,
	},
	"riak": {
		URL:                 charm.MustParseURL("cs:~charmers/" + SearchSeries[2] + "/riak-67"),
		PromulgatedRevision: 67,
		Charm: NewCharm(&charm.Meta{
			Categories: []string{"riak"},
			Tags:       []string{"riakTAG"},
		}),
		ACL: []string{"charmers"},
	},
	"wordpress-simple": {
		URL:                 charm.MustParseURL("cs:~charmers/bundle/wordpress-simple-4"),
		PromulgatedRevision: 4,
		Bundle: NewBundle(&charm.BundleData{
			Applications: map[string]*charm.ApplicationSpec{
				"wordpress": {
					Charm:    "wordpress",
					NumUnits: 1,
				},
			},
			Tags: []string{"wordpress"},
		}),
		ACL:       []string{params.Everyone},
		Downloads: 1,
	},
	// Note: "squid-forwardproxy" shares a trigram "dpr" with "wordpress".
	"squid-forwardproxy": {
		URL:                 charm.MustParseURL("cs:~charmers/" + SearchSeries[2] + "/squid-forwardproxy-3"),
		PromulgatedRevision: 3,
		Charm:               NewCharm(&charm.Meta{}),
		ACL:                 []string{params.Everyone},
		Downloads:           2,
	},
	// Note: "cloud-controller-worker-v2" shares a trigram "wor" with "wordpress".
	"cloud-controller-worker-v2": {
		URL:                 charm.MustParseURL("cs:~cf-charmers/" + SearchSeries[1] + "/cloud-controller-worker-v2-7"),
		PromulgatedRevision: -1,
		Charm:               NewCharm(&charm.Meta{}),
		ACL:                 []string{params.Everyone},
		Downloads:           4,
	},
	"multi-series": {
		URL:                 charm.MustParseURL("cs:~charmers/multi-series-0"),
		PromulgatedRevision: 0,
		Charm: NewCharm(&charm.Meta{
			Series: SearchSeries,
			Provides: map[string]charm.Relation{
				"url": {
					Name:      "url",
					Interface: "http",
					Scope:     charm.ScopeGlobal,
				},
				"monitoring-port": {
					Name:      "monitoring-port",
					Interface: "monitoring",
					Scope:     charm.ScopeContainer,
				},
			},
			Requires: map[string]charm.Relation{
				"mysql": {
					Name:      "mysql",
					Interface: "mysql",
					Scope:     charm.ScopeGlobal,
				},
				"cache": {
					Name:      "cache",
					Interface: "varnish",
					Scope:     charm.ScopeGlobal,
				},
			},
			Categories: []string{"multi-series", "multi-seriesCAT"},
			Tags:       []string{"multi-seriesTAG"},
		}),
		ACL: []string{params.Everyone},
	},
}

// SortBySeries sorts the given slice of SearchEntities by series name
// and returns the slice.
func SortBySeries(es []SearchEntity, desc bool) []SearchEntity {
	f := func(i, j int) bool { return lowestSeries(es[i]) < lowestSeries(es[j]) }
	if desc {
		f = func(i, j int) bool { return highestSeries(es[j]) < highestSeries(es[i]) }
	}
	sort.SliceStable(es, f)
	return es
}

func lowestSeries(e SearchEntity) string {
	if e.URL.Series != "" {
		return e.URL.Series
	}
	s := e.Charm.Meta().Series[0]
	for _, t := range e.Charm.Meta().Series[1:] {
		if t < s {
			s = t
		}
	}
	return s
}

func highestSeries(e SearchEntity) string {
	if e.URL.Series != "" {
		return e.URL.Series
	}
	s := e.Charm.Meta().Series[0]
	for _, t := range e.Charm.Meta().Series[1:] {
		if t > s {
			s = t
		}
	}
	return s
}

// ResolvedURLs creates a list of ResolvedURLs for the given
// SearchEntities. If expand is true then all the URLs will be expanded
// to include all possible series. This if for v4 compatibility.
func ResolvedURLs(es []SearchEntity, expand bool) []*router.ResolvedURL {
	urls := make([]*router.ResolvedURL, 0, len(es))
	for _, e := range es {
		rurl := e.ResolvedURL()
		if !expand || rurl.URL.Series != "" {
			urls = append(urls, rurl)
			continue
		}
		for _, s := range e.Charm.Meta().Series {
			urls = append(urls, ResolvedURLWithSeries(rurl, s))
		}
	}
	return urls
}

// ResolvedURLWithSeries creates a copy of the given ResolvedURL with the
// given series.
func ResolvedURLWithSeries(rurl *router.ResolvedURL, series string) *router.ResolvedURL {
	rurl1 := *rurl
	rurl1.URL.Series = series
	return &rurl1
}

// SortResolvedURLsBySeries sorts the given slice of ResolvedURLs in
// series order.
func SortResolvedURLsBySeries(urls []*router.ResolvedURL, desc bool) []*router.ResolvedURL {
	f := func(i, j int) bool { return urls[i].URL.Series < urls[j].URL.Series }
	if desc {
		f = func(i, j int) bool { return urls[j].URL.Series < urls[i].URL.Series }
	}
	sort.SliceStable(urls, f)
	return urls
}
