// Copyright 2014 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package v4

import (
	"net/http"
	"strconv"
	"sync/atomic"

	"github.com/juju/utils/parallel"
	"gopkg.in/errgo.v1"

	"gopkg.in/juju/charmstore.v5/internal/charmstore"
	"gopkg.in/juju/charmstore.v5/internal/router"
	"gopkg.in/juju/charmrepo.v0/csclient/params"
)

const maxConcurrency = 20

// GET search[?text=text][&autocomplete=1][&filter=value…][&limit=limit][&include=meta][&skip=count][&sort=field[+dir]]
// https://github.com/juju/charmstore/blob/v4/docs/API.md#get-search
func (h *Handler) serveSearch(_ http.Header, req *http.Request) (interface{}, error) {
	sp, err := parseSearchParams(req)
	if err != nil {
		return "", err
	}
	auth, err := h.checkRequest(req, nil)
	if err != nil {
		logger.Infof("authorization failed on search request, granting no privileges: %v", err)
	}
	sp.Admin = auth.Admin
	if auth.Username != "" {
		sp.Groups = append(sp.Groups, auth.Username)
		groups, err := h.groupsForUser(auth.Username)
		if err != nil {
			logger.Infof("cannot get groups for user %q, assuming no groups: %v", auth.Username, err)
		}
		sp.Groups = append(sp.Groups, groups...)
	}
	// perform query
	store := h.pool.Store()
	defer store.Close()
	results, err := store.Search(sp)
	if err != nil {
		return nil, errgo.Notef(err, "error performing search")
	}
	response := params.SearchResponse{
		SearchTime: results.SearchTime,
		Total:      results.Total,
		Results:    make([]params.SearchResult, len(results.Results)),
	}
	run := parallel.NewRun(maxConcurrency)
	var missing int32
	for i, ref := range results.Results {
		i, ref := i, ref
		run.Do(func() error {
			meta, err := h.Router.GetMetadata(ref, sp.Include, req)
			if err != nil {
				// Unfortunately it is possible to get errors here due to
				// internal inconsistency, so rather than throwing away
				// all the search results, we just log the error and move on.
				logger.Errorf("cannot retrieve metadata for %v: %v", ref, err)
				atomic.AddInt32(&missing, 1)
				return nil
			}
			response.Results[i] = params.SearchResult{
				Id:   ref.PreferredURL(),
				Meta: meta,
			}
			return nil
		})
	}
	// We never return an error from the Do function above, so no need to
	// check the error here.
	run.Wait()
	if missing == 0 {
		return response, nil
	}
	// We're missing some results - shuffle all the results down to
	// fill the gaps.
	j := 0
	for _, result := range response.Results {
		if result.Id != nil {
			response.Results[j] = result
			j++
		}
	}
	response.Results = response.Results[0:j]
	return response, nil
}

// GET search/interesting[?limit=limit][&include=meta]
// https://github.com/juju/charmstore/blob/v4/docs/API.md#get-searchinteresting
func (h *Handler) serveSearchInteresting(w http.ResponseWriter, req *http.Request) {
	router.WriteError(w, errNotImplemented)
}

// parseSearchParms extracts the search paramaters from the request
func parseSearchParams(req *http.Request) (charmstore.SearchParams, error) {
	sp := charmstore.SearchParams{}
	var err error
	for k, v := range req.Form {
		switch k {
		case "text":
			sp.Text = v[0]
		case "autocomplete":
			sp.AutoComplete, err = parseBool(v[0])
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
			promulgated, err := parseBool(v[0])
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
