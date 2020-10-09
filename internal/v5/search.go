// Copyright 2014 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package v5 // import "gopkg.in/juju/charmstore.v5/internal/v5"

import (
	"net/http"
	"strconv"

	"github.com/juju/charmrepo/v6/csclient/params"
	"golang.org/x/net/context"
	"gopkg.in/errgo.v1"

	"gopkg.in/juju/charmstore.v5/internal/charmstore"
	"gopkg.in/juju/charmstore.v5/internal/mongodoc"
	"gopkg.in/juju/charmstore.v5/internal/router"
)

const maxConcurrency = 20

// GET search[?text=text][&autocomplete=1][&filter=value…][&limit=limit][&include=meta][&skip=count][&sort=field[+dir]]
// https://github.com/juju/charmstore/blob/v4/docs/API.md#get-search
func (h *ReqHandler) serveSearch(_ http.Header, req *http.Request) (interface{}, error) {
	sp, err := ParseSearchParams(req)
	if err != nil {
		return "", err
	}
	auth, err := h.Authenticate(req)
	if err != nil {
		logger.Infof("authorization failed on search request, granting no privileges: %v", err)
	}
	sp.Admin = auth.Admin
	if auth.User != nil {
		sp.Groups = append(sp.Groups, auth.Username)
		groups, err := auth.User.Groups()
		if err != nil {
			logger.Infof("cannot get groups for user %q, assuming no groups: %v", auth.Username, err)
		}
		sp.Groups = append(sp.Groups, groups...)
	}
	return h.Search(sp, req)
}

// Search performs the search specified by SearchParams. If sp
// specifies that additional metadata needs to be added to the results,
// then it is added.
func (h *ReqHandler) Search(sp charmstore.SearchParams, req *http.Request) (interface{}, error) {
	// perform query
	h.WillIncludeMetadata(sp.Include)
	query := h.Store.SearchQuery(sp)

	var entities []*mongodoc.Entity
	iter := h.Cache.CustomIter(query, nil)
	defer iter.Close()
	for iter.Next() {
		entities = append(entities, iter.Entity())
	}
	if iter.Err() != nil {
		return nil, errgo.Notef(iter.Err(), "error performing search")
	}
	results, err := h.getMetadataForEntities(entities, sp.Include, req, nil)
	if err != nil {
		return nil, errgo.Notef(err, "cannot get metadata")
	}
	return params.SearchResponse{
		SearchTime: query.Duration(),
		Total:      query.Total(),
		Results:    results,
	}, nil
}

// GET search/interesting[?limit=limit][&include=meta]
// https://github.com/juju/charmstore/blob/v4/docs/API.md#get-searchinteresting
func (h *ReqHandler) serveSearchInteresting(w http.ResponseWriter, req *http.Request) {
	router.WriteError(context.TODO(), w, errNotImplemented)
}

// ParseSearchParms extracts the search paramaters from the request
func ParseSearchParams(req *http.Request) (charmstore.SearchParams, error) {
	sp := charmstore.SearchParams{}
	sp.AutoComplete = true
	var err error
	for k, v := range req.Form {
		switch k {
		case "text":
			sp.Text = v[0]
		case "autocomplete":
			sp.AutoComplete, err = router.ParseBool(v[0])
			if err != nil {
				return charmstore.SearchParams{}, badRequestf(err, "invalid autocomplete parameter")
			}
		case "limit":
			sp.Limit, err = strconv.Atoi(v[0])
			if err != nil {
				return charmstore.SearchParams{}, badRequestf(err, "invalid limit parameter: could not parse integer")
			}
			if sp.Limit < 1 {
				return charmstore.SearchParams{}, badRequestf(nil, "invalid limit parameter: expected integer greater than zero")
			}
		case "include":
			for _, s := range v {
				if s != "" {
					sp.Include = append(sp.Include, s)
				}
			}
		case "description", "name", "owner", "provides", "requires", "series", "summary", "tags", "type":
			if sp.Filters == nil {
				sp.Filters = make(map[string][]string)
			}
			sp.Filters[k] = v
		case "promulgated":
			promulgated, err := router.ParseBool(v[0])
			if err != nil {
				return charmstore.SearchParams{}, badRequestf(err, "invalid promulgated filter parameter")
			}
			if sp.Filters == nil {
				sp.Filters = make(map[string][]string)
			}
			if promulgated {
				sp.Filters[k] = []string{"1"}
			} else {
				sp.Filters[k] = []string{"0"}
			}
		case "skip":
			sp.Skip, err = strconv.Atoi(v[0])
			if err != nil {
				return charmstore.SearchParams{}, badRequestf(err, "invalid skip parameter: could not parse integer")
			}
			if sp.Skip < 0 {
				return charmstore.SearchParams{}, badRequestf(nil, "invalid skip parameter: expected non-negative integer")
			}
		case "sort":
			err = sp.ParseSortFields(v...)
			if err != nil {
				return charmstore.SearchParams{}, badRequestf(err, "invalid sort field")
			}
		default:
			return charmstore.SearchParams{}, badRequestf(nil, "invalid parameter: %s", k)
		}
	}
	return sp, nil
}
