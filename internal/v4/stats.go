// Copyright 2012 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package v4

import (
	"net/http"
	"net/url"
	"strings"
	"time"

	"gopkg.in/errgo.v1"

	"gopkg.in/juju/charmstore.v5/internal/charmstore"
	"gopkg.in/juju/charmrepo.v0/csclient/params"
)

const dateFormat = "2006-01-02"

// parseDateRange parses a date range as specified in an http
// request. The returned times will be zero if not specified.
func parseDateRange(form url.Values) (start, stop time.Time, err error) {
	if v := form.Get("start"); v != "" {
		var err error
		start, err = time.Parse(dateFormat, v)
		if err != nil {
			return time.Time{}, time.Time{}, badRequestf(err, "invalid 'start' value %q", v)
		}
	}
	if v := form.Get("stop"); v != "" {
		var err error
		stop, err = time.Parse(dateFormat, v)
		if err != nil {
			return time.Time{}, time.Time{}, badRequestf(err, "invalid 'stop' value %q", v)
		}
		// Cover all timestamps within the stop day.
		stop = stop.Add(24*time.Hour - 1*time.Second)
	}
	return
}

// GET stats/counter/key[:key]...?[by=unit]&start=date][&stop=date][&list=1]
// https://github.com/juju/charmstore/blob/v4/docs/API.md#get-statscounter
func (h *Handler) serveStatsCounter(_ http.Header, r *http.Request) (interface{}, error) {
	base := strings.TrimPrefix(r.URL.Path, "/")
	if strings.Index(base, "/") > 0 {
		return nil, errgo.WithCausef(nil, params.ErrNotFound, "invalid key")
	}
	if base == "" {
		return nil, params.ErrForbidden
	}
	var by charmstore.CounterRequestBy
	switch v := r.Form.Get("by"); v {
	case "":
		by = charmstore.ByAll
	case "day":
		by = charmstore.ByDay
	case "week":
		by = charmstore.ByWeek
	default:
		return nil, badRequestf(nil, "invalid 'by' value %q", v)
	}
	req := charmstore.CounterRequest{
		Key:  strings.Split(base, ":"),
		List: r.Form.Get("list") == "1",
		By:   by,
	}
	var err error
	req.Start, req.Stop, err = parseDateRange(r.Form)
	if err != nil {
		return nil, errgo.Mask(err, errgo.Is(params.ErrBadRequest))
	}
	if req.Key[len(req.Key)-1] == "*" {
		req.Prefix = true
		req.Key = req.Key[:len(req.Key)-1]
		if len(req.Key) == 0 {
			return nil, errgo.WithCausef(nil, params.ErrForbidden, "unknown key")
		}
	}
	store := h.pool.Store()
	defer store.Close()
	entries, err := store.Counters(&req)
	if err != nil {
		return nil, errgo.Notef(err, "cannot query counters")
	}

	var buf []byte
	var items []params.Statistic
	for i := range entries {
		entry := &entries[i]
		buf = buf[:0]
		if req.List {
			for j := range entry.Key {
				buf = append(buf, entry.Key[j]...)
				buf = append(buf, ':')
			}
			if entry.Prefix {
				buf = append(buf, '*')
			} else {
				buf = buf[:len(buf)-1]
			}
		}
		stat := params.Statistic{
			Key:   string(buf),
			Count: entry.Count,
		}
		if !entry.Time.IsZero() {
			stat.Date = entry.Time.Format("2006-01-02")
		}
		items = append(items, stat)
	}

	return items, nil
}

// StatsEnabled reports whether statistics should be gathered for
// the given HTTP request.
func StatsEnabled(req *http.Request) bool {
	// It's fine to parse the form more than once, and it avoids
	// bugs from not parsing it.
	req.ParseForm()
	return req.Form.Get("stats") != "0"
}
