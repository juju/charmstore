// Copyright 2012, 2013 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package charmstore_test

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"time"

	jc "github.com/juju/testing/checkers"
	"gopkg.in/juju/charm.v2"
	"labix.org/v2/mgo/bson"
	gc "launchpad.net/gocheck"

	"github.com/juju/charmstore"
)

func (s *StoreSuite) prepareServer(c *gc.C) (*charmstore.Server, *charm.URL) {
	curl := charm.MustParseURL("cs:precise/wordpress")
	pub, err := s.store.CharmPublisher([]*charm.URL{curl}, "some-digest")
	c.Assert(err, gc.IsNil)
	err = pub.Publish(&FakeCharmDir{})
	c.Assert(err, gc.IsNil)

	server, err := charmstore.NewServer(s.store)
	c.Assert(err, gc.IsNil)
	return server, curl
}

func (s *StoreSuite) TestServerCharmInfo(c *gc.C) {
	server, curl := s.prepareServer(c)

	var tests = []struct{ url, canonical, sha, digest, err string }{
		{curl.String(), curl.String(), fakeRevZeroSha, "some-digest", ""},
		{"cs:oneiric/non-existent", "", "", "", "entry not found"},
		{"cs:wordpress", curl.String(), fakeRevZeroSha, "some-digest", ""},
		{"cs:/bad", "", "", "", `charm URL has invalid series: "cs:/bad"`},
		{"gopher:archie-server", "", "", "", `charm URL has invalid schema: "gopher:archie-server"`},
	}

	for i, t := range tests {
		c.Logf("test %d: %s", i, t.url)
		req, err := http.NewRequest("GET", "/charm-info", nil)
		c.Assert(err, gc.IsNil)
		req.Form = url.Values{"charms": []string{t.url}}
		rec := httptest.NewRecorder()
		server.ServeHTTP(rec, req)

		expected := make(map[string]interface{})
		if t.sha != "" {
			expected[t.url] = map[string]interface{}{
				"canonical-url": t.canonical,
				"revision":      float64(0),
				"sha256":        t.sha,
				"digest":        t.digest,
			}
		} else {
			expected[t.url] = map[string]interface{}{
				"revision": float64(0),
				"errors":   []interface{}{t.err},
			}
		}
		obtained := map[string]interface{}{}
		c.Logf("got %s\n", rec.Body.Bytes())
		err = json.NewDecoder(rec.Body).Decode(&obtained)
		c.Assert(err, gc.IsNil)
		c.Assert(obtained, gc.DeepEquals, expected, gc.Commentf("URL: %s", t.url))
		c.Assert(rec.Header().Get("Content-Type"), gc.Equals, "application/json")
	}

	// 2 charm-info events, one for resolved URL, one for the reference.
	s.checkCounterSum(c, []string{"charm-info", curl.Series, curl.Name}, false, 2)
	s.checkCounterSum(c, []string{"charm-missing", "oneiric", "non-existent"}, false, 1)
}

func (s *StoreSuite) TestServerCharmEvent(c *gc.C) {
	server, _ := s.prepareServer(c)

	url1 := charm.MustParseURL("cs:oneiric/wordpress")
	url2 := charm.MustParseURL("cs:oneiric/mysql")
	urls := []*charm.URL{url1, url2}

	event1 := &charmstore.CharmEvent{
		Kind:     charmstore.EventPublished,
		Revision: 42,
		Digest:   "revKey1",
		URLs:     urls,
		Warnings: []string{"A warning."},
		Time:     time.Unix(1, 0),
	}
	event2 := &charmstore.CharmEvent{
		Kind:     charmstore.EventPublished,
		Revision: 43,
		Digest:   "revKey2",
		URLs:     urls,
		Time:     time.Unix(2, 0),
	}
	event3 := &charmstore.CharmEvent{
		Kind:   charmstore.EventPublishError,
		Digest: "revKey3",
		Errors: []string{"An error."},
		URLs:   urls[:1],
		Time:   time.Unix(3, 0),
	}

	for _, event := range []*charmstore.CharmEvent{event1, event2, event3} {
		err := s.store.LogCharmEvent(event)
		c.Assert(err, gc.IsNil)
	}

	var tests = []struct {
		query        string
		kind, digest string
		err, warn    string
		time         string
		revision     int
	}{
		{
			query:  url1.String(),
			digest: "revKey3",
			kind:   "publish-error",
			err:    "An error.",
			time:   "1970-01-01T00:00:03Z",
		}, {
			query:    url2.String(),
			digest:   "revKey2",
			kind:     "published",
			revision: 43,
			time:     "1970-01-01T00:00:02Z",
		}, {
			query:    url1.String() + "@revKey1",
			digest:   "revKey1",
			kind:     "published",
			revision: 42,
			warn:     "A warning.",
			time:     "1970-01-01T00:00:01Z",
		}, {
			query:    "cs:non/existent",
			revision: 0,
			err:      "entry not found",
		},
	}

	for _, t := range tests {
		req, err := http.NewRequest("GET", "/charm-event", nil)
		c.Assert(err, gc.IsNil)
		req.Form = url.Values{"charms": []string{t.query}}
		rec := httptest.NewRecorder()
		server.ServeHTTP(rec, req)

		url := t.query
		if i := strings.Index(url, "@"); i >= 0 {
			url = url[:i]
		}
		info := map[string]interface{}{
			"kind":     "",
			"revision": float64(0),
		}
		if t.kind != "" {
			info["kind"] = t.kind
			info["revision"] = float64(t.revision)
			info["digest"] = t.digest
			info["time"] = t.time
		}
		if t.err != "" {
			info["errors"] = []interface{}{t.err}
		}
		if t.warn != "" {
			info["warnings"] = []interface{}{t.warn}
		}
		expected := map[string]interface{}{url: info}
		obtained := map[string]interface{}{}
		err = json.NewDecoder(rec.Body).Decode(&obtained)
		c.Assert(err, gc.IsNil)
		c.Assert(obtained, gc.DeepEquals, expected)
		c.Assert(rec.Header().Get("Content-Type"), gc.Equals, "application/json")
	}

	s.checkCounterSum(c, []string{"charm-event", "oneiric", "wordpress"}, false, 2)
	s.checkCounterSum(c, []string{"charm-event", "oneiric", "mysql"}, false, 1)

	query1 := url1.String() + "@" + event1.Digest
	query3 := url1.String() + "@" + event3.Digest
	event1Info := map[string]interface{}{
		"kind":     "published",
		"revision": float64(42),
		"digest":   "revKey1",
		"warnings": []interface{}{"A warning."},
		"time":     "1970-01-01T00:00:01Z"}
	event3Info := map[string]interface{}{
		"kind":     "publish-error",
		"revision": float64(0),
		"digest":   "revKey3",
		"errors":   []interface{}{"An error."},
		"time":     "1970-01-01T00:00:03Z"}

	req, err := http.NewRequest("GET", "/charm-event", nil)
	c.Assert(err, gc.IsNil)
	req.Form = url.Values{"charms": []string{query1, query3}}
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)
	expected := map[string]interface{}{url1.String(): event3Info}
	obtained := map[string]interface{}{}
	err = json.NewDecoder(rec.Body).Decode(&obtained)
	c.Assert(err, gc.IsNil)
	c.Assert(obtained, jc.DeepEquals, expected)

	req, err = http.NewRequest("GET", "/charm-event", nil)
	c.Assert(err, gc.IsNil)
	req.Form = url.Values{"charms": []string{query1, query3}, "long_keys": []string{"1"}}
	rec = httptest.NewRecorder()
	server.ServeHTTP(rec, req)
	expected = map[string]interface{}{query1: event1Info, query3: event3Info}
	obtained = map[string]interface{}{}
	err = json.NewDecoder(rec.Body).Decode(&obtained)
	c.Assert(err, gc.IsNil)
	c.Assert(obtained, jc.DeepEquals, expected)
}

func (s *StoreSuite) TestSeriesNotFound(c *gc.C) {
	server, err := charmstore.NewServer(s.store)
	req, err := http.NewRequest("GET", "/charm-info?charms=cs:not-found", nil)
	c.Assert(err, gc.IsNil)
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)
	c.Assert(rec.Code, gc.Equals, http.StatusOK)

	expected := map[string]interface{}{"cs:not-found": map[string]interface{}{
		"revision": float64(0),
		"errors":   []interface{}{"entry not found"}}}
	obtained := map[string]interface{}{}
	err = json.NewDecoder(rec.Body).Decode(&obtained)
	c.Assert(err, gc.IsNil)
	c.Assert(obtained, gc.DeepEquals, expected)
}

// checkCounterSum checks that statistics are properly collected.
// It retries a few times as they are generally collected in background.
func (s *StoreSuite) checkCounterSum(c *gc.C, key []string, prefix bool, expected int64) {
	if *noTestMongoJs {
		c.Skip("MongoDB javascript not available")
	}

	var sum int64
	for retry := 0; retry < 10; retry++ {
		time.Sleep(1e8)
		req := charmstore.CounterRequest{Key: key, Prefix: prefix}
		cs, err := s.store.Counters(&req)
		c.Assert(err, gc.IsNil)
		if sum = cs[0].Count; sum == expected {
			if expected == 0 && retry < 2 {
				continue // Wait a bit to make sure.
			}
			return
		}
	}
	c.Errorf("counter sum for %#v is %d, want %d", key, sum, expected)
}

func (s *StoreSuite) TestCharmStreaming(c *gc.C) {
	server, curl := s.prepareServer(c)

	req, err := http.NewRequest("GET", "/charm/"+curl.String()[3:], nil)
	c.Assert(err, gc.IsNil)
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	data, err := ioutil.ReadAll(rec.Body)
	c.Assert(string(data), gc.Equals, "charm-revision-0")

	c.Assert(rec.Header().Get("Connection"), gc.Equals, "close")
	c.Assert(rec.Header().Get("Content-Type"), gc.Equals, "application/octet-stream")
	c.Assert(rec.Header().Get("Content-Length"), gc.Equals, "16")

	// Check that it was accounted for in statistics.
	s.checkCounterSum(c, []string{"charm-bundle", curl.Series, curl.Name}, false, 1)
}

func (s *StoreSuite) TestDisableStats(c *gc.C) {
	server, curl := s.prepareServer(c)

	req, err := http.NewRequest("GET", "/charm-info", nil)
	c.Assert(err, gc.IsNil)
	req.Form = url.Values{"charms": []string{curl.String()}, "stats": []string{"0"}}
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)
	c.Assert(rec.Code, gc.Equals, 200)

	req, err = http.NewRequest("GET", "/charm/"+curl.String()[3:], nil)
	c.Assert(err, gc.IsNil)
	req.Form = url.Values{"stats": []string{"0"}}
	rec = httptest.NewRecorder()
	server.ServeHTTP(rec, req)
	c.Assert(rec.Code, gc.Equals, 200)

	// No statistics should have been collected given the use of stats=0.
	for _, prefix := range []string{"charm-info", "charm-bundle", "charm-missing"} {
		s.checkCounterSum(c, []string{prefix}, true, 0)
	}
}

func (s *StoreSuite) TestServerStatus(c *gc.C) {
	server, err := charmstore.NewServer(s.store)
	c.Assert(err, gc.IsNil)
	tests := []struct {
		path string
		code int
	}{
		{"/charm-info/any", 404},
		{"/charm/bad-url", 404},
		{"/charm/bad-series/wordpress", 404},
		{"/stats/counter/", 403},
		{"/stats/counter/*", 403},
		{"/stats/counter/any/", 404},
		{"/stats/", 404},
		{"/stats/any", 404},
	}
	for _, test := range tests {
		req, err := http.NewRequest("GET", test.path, nil)
		c.Assert(err, gc.IsNil)
		rec := httptest.NewRecorder()
		server.ServeHTTP(rec, req)
		c.Assert(rec.Code, gc.Equals, test.code, gc.Commentf("Path: %s", test.path))
	}
}

func (s *StoreSuite) TestRootRedirect(c *gc.C) {
	server, err := charmstore.NewServer(s.store)
	c.Assert(err, gc.IsNil)
	req, err := http.NewRequest("GET", "/", nil)
	c.Assert(err, gc.IsNil)
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)
	c.Assert(rec.Code, gc.Equals, 303)
	c.Assert(rec.Header().Get("Location"), gc.Equals, "https://juju.ubuntu.com")
}

func (s *StoreSuite) TestStatsCounter(c *gc.C) {
	if *noTestMongoJs {
		c.Skip("MongoDB javascript not available")
	}

	for _, key := range [][]string{{"a", "b"}, {"a", "b"}, {"a", "c"}, {"a"}} {
		err := s.store.IncCounter(key)
		c.Assert(err, gc.IsNil)
	}

	server, _ := s.prepareServer(c)

	expected := map[string]string{
		"a:b":   "2",
		"a:b:*": "0",
		"a:*":   "3",
		"a":     "1",
		"a:b:c": "0",
	}

	for counter, n := range expected {
		req, err := http.NewRequest("GET", "/stats/counter/"+counter, nil)
		c.Assert(err, gc.IsNil)
		rec := httptest.NewRecorder()
		server.ServeHTTP(rec, req)

		data, err := ioutil.ReadAll(rec.Body)
		c.Assert(string(data), gc.Equals, n)

		c.Assert(rec.Header().Get("Content-Type"), gc.Equals, "text/plain")
		c.Assert(rec.Header().Get("Content-Length"), gc.Equals, strconv.Itoa(len(n)))
	}
}

func (s *StoreSuite) TestStatsCounterList(c *gc.C) {
	if *noTestMongoJs {
		c.Skip("MongoDB javascript not available")
	}

	incs := [][]string{
		{"a"},
		{"a", "b"},
		{"a", "b", "c"},
		{"a", "b", "c"},
		{"a", "b", "d"},
		{"a", "b", "e"},
		{"a", "f", "g"},
		{"a", "f", "h"},
		{"a", "i"},
		{"j", "k"},
	}
	for _, key := range incs {
		err := s.store.IncCounter(key)
		c.Assert(err, gc.IsNil)
	}

	server, _ := s.prepareServer(c)

	tests := []struct {
		key, format, result string
	}{
		{"a", "", "a  1\n"},
		{"a:*", "", "a:b:*  4\na:f:*  2\na:b    1\na:i    1\n"},
		{"a:b:*", "", "a:b:c  2\na:b:d  1\na:b:e  1\n"},
		{"a:*", "csv", "a:b:*,4\na:f:*,2\na:b,1\na:i,1\n"},
		{"a:*", "json", `[["a:b:*",4],["a:f:*",2],["a:b",1],["a:i",1]]`},
	}

	for _, test := range tests {
		req, err := http.NewRequest("GET", "/stats/counter/"+test.key, nil)
		c.Assert(err, gc.IsNil)
		req.Form = url.Values{"list": []string{"1"}}
		if test.format != "" {
			req.Form.Set("format", test.format)
		}
		rec := httptest.NewRecorder()
		server.ServeHTTP(rec, req)

		data, err := ioutil.ReadAll(rec.Body)
		c.Assert(string(data), gc.Equals, test.result)

		c.Assert(rec.Header().Get("Content-Type"), gc.Equals, "text/plain")
		c.Assert(rec.Header().Get("Content-Length"), gc.Equals, strconv.Itoa(len(test.result)))
	}
}

func (s *StoreSuite) TestStatsCounterBy(c *gc.C) {
	if *noTestMongoJs {
		c.Skip("MongoDB javascript not available")
	}

	incs := []struct {
		key []string
		day int
	}{
		{[]string{"a"}, 1},
		{[]string{"a"}, 1},
		{[]string{"b"}, 1},
		{[]string{"a", "b"}, 1},
		{[]string{"a", "c"}, 1},
		{[]string{"a"}, 3},
		{[]string{"a", "b"}, 3},
		{[]string{"b"}, 9},
		{[]string{"b"}, 9},
		{[]string{"a", "c", "d"}, 9},
		{[]string{"a", "c", "e"}, 9},
		{[]string{"a", "c", "f"}, 9},
	}

	day := func(i int) time.Time {
		return time.Date(2012, time.May, i, 0, 0, 0, 0, time.UTC)
	}

	server, _ := s.prepareServer(c)

	counters := s.Session.DB("juju").C("stat.counters")
	for i, inc := range incs {
		err := s.store.IncCounter(inc.key)
		c.Assert(err, gc.IsNil)

		// Hack time so counters are assigned to 2012-05-<day>
		filter := bson.M{"t": bson.M{"$gt": charmstore.TimeToStamp(time.Date(2013, time.January, 1, 0, 0, 0, 0, time.UTC))}}
		stamp := charmstore.TimeToStamp(day(inc.day))
		stamp += int32(i) * 60 // Make every entry unique.
		err = counters.Update(filter, bson.D{{"$set", bson.D{{"t", stamp}}}})
		c.Check(err, gc.IsNil)
	}

	tests := []struct {
		request charmstore.CounterRequest
		format  string
		result  string
	}{
		{
			charmstore.CounterRequest{
				Key:    []string{"a"},
				Prefix: false,
				List:   false,
				By:     charmstore.ByDay,
			},
			"",
			"2012-05-01  2\n2012-05-03  1\n",
		}, {
			charmstore.CounterRequest{
				Key:    []string{"a"},
				Prefix: false,
				List:   false,
				By:     charmstore.ByDay,
			},
			"csv",
			"2012-05-01,2\n2012-05-03,1\n",
		}, {
			charmstore.CounterRequest{
				Key:    []string{"a"},
				Prefix: false,
				List:   false,
				By:     charmstore.ByDay,
			},
			"json",
			`[["2012-05-01",2],["2012-05-03",1]]`,
		}, {
			charmstore.CounterRequest{
				Key:    []string{"a"},
				Prefix: true,
				List:   false,
				By:     charmstore.ByDay,
			},
			"",
			"2012-05-01  2\n2012-05-03  1\n2012-05-09  3\n",
		}, {
			charmstore.CounterRequest{
				Key:    []string{"a"},
				Prefix: true,
				List:   false,
				By:     charmstore.ByDay,
				Start:  time.Date(2012, 5, 2, 0, 0, 0, 0, time.UTC),
			},
			"",
			"2012-05-03  1\n2012-05-09  3\n",
		}, {
			charmstore.CounterRequest{
				Key:    []string{"a"},
				Prefix: true,
				List:   false,
				By:     charmstore.ByDay,
				Stop:   time.Date(2012, 5, 4, 0, 0, 0, 0, time.UTC),
			},
			"",
			"2012-05-01  2\n2012-05-03  1\n",
		}, {
			charmstore.CounterRequest{
				Key:    []string{"a"},
				Prefix: true,
				List:   false,
				By:     charmstore.ByDay,
				Start:  time.Date(2012, 5, 3, 0, 0, 0, 0, time.UTC),
				Stop:   time.Date(2012, 5, 3, 0, 0, 0, 0, time.UTC),
			},
			"",
			"2012-05-03  1\n",
		}, {
			charmstore.CounterRequest{
				Key:    []string{"a"},
				Prefix: true,
				List:   true,
				By:     charmstore.ByDay,
			},
			"",
			"a:b    2012-05-01  1\na:c    2012-05-01  1\na:b    2012-05-03  1\na:c:*  2012-05-09  3\n",
		}, {
			charmstore.CounterRequest{
				Key:    []string{"a"},
				Prefix: true,
				List:   false,
				By:     charmstore.ByWeek,
			},
			"",
			"2012-05-06  3\n2012-05-13  3\n",
		}, {
			charmstore.CounterRequest{
				Key:    []string{"a"},
				Prefix: true,
				List:   true,
				By:     charmstore.ByWeek,
			},
			"",
			"a:b    2012-05-06  2\na:c    2012-05-06  1\na:c:*  2012-05-13  3\n",
		}, {
			charmstore.CounterRequest{
				Key:    []string{"a"},
				Prefix: true,
				List:   true,
				By:     charmstore.ByWeek,
			},
			"csv",
			"a:b,2012-05-06,2\na:c,2012-05-06,1\na:c:*,2012-05-13,3\n",
		}, {
			charmstore.CounterRequest{
				Key:    []string{"a"},
				Prefix: true,
				List:   true,
				By:     charmstore.ByWeek,
			},
			"json",
			`[["a:b","2012-05-06",2],["a:c","2012-05-06",1],["a:c:*","2012-05-13",3]]`,
		},
	}

	for _, test := range tests {
		path := "/stats/counter/" + strings.Join(test.request.Key, ":")
		if test.request.Prefix {
			path += ":*"
		}
		req, err := http.NewRequest("GET", path, nil)
		req.Form = url.Values{}
		c.Assert(err, gc.IsNil)
		if test.request.List {
			req.Form.Set("list", "1")
		}
		if test.format != "" {
			req.Form.Set("format", test.format)
		}
		if !test.request.Start.IsZero() {
			req.Form.Set("start", test.request.Start.Format("2006-01-02"))
		}
		if !test.request.Stop.IsZero() {
			req.Form.Set("stop", test.request.Stop.Format("2006-01-02"))
		}
		switch test.request.By {
		case charmstore.ByDay:
			req.Form.Set("by", "day")
		case charmstore.ByWeek:
			req.Form.Set("by", "week")
		}
		rec := httptest.NewRecorder()
		server.ServeHTTP(rec, req)

		data, err := ioutil.ReadAll(rec.Body)
		c.Assert(string(data), gc.Equals, test.result)

		c.Assert(rec.Header().Get("Content-Type"), gc.Equals, "text/plain")
		c.Assert(rec.Header().Get("Content-Length"), gc.Equals, strconv.Itoa(len(test.result)))
	}
}

func (s *StoreSuite) TestBlitzKey(c *gc.C) {
	server, _ := s.prepareServer(c)

	// This is just a validation key to allow blitz.io to run
	// performance tests against the site.
	req, err := http.NewRequest("GET", "/mu-35700a31-6bf320ca-a800b670-05f845ee", nil)
	c.Assert(err, gc.IsNil)
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	data, err := ioutil.ReadAll(rec.Body)
	c.Assert(string(data), gc.Equals, "42")

	c.Assert(rec.Header().Get("Connection"), gc.Equals, "close")
	c.Assert(rec.Header().Get("Content-Type"), gc.Equals, "text/plain")
	c.Assert(rec.Header().Get("Content-Length"), gc.Equals, "2")
}
