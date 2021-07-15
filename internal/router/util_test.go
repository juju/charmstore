// Copyright 2014 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package router_test

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"

	jujutesting "github.com/juju/testing"
	jc "github.com/juju/testing/checkers"
	gc "gopkg.in/check.v1"
	"gopkg.in/errgo.v1"

	"gopkg.in/juju/charmstore.v5/internal/router"
)

type utilSuite struct {
	jujutesting.LoggingSuite
}

var _ = gc.Suite(&utilSuite{})
var relativeURLTests = []struct {
	base        string
	target      string
	expect      string
	expectError string
}{{
	expectError: "non-absolute base URL",
}, {
	base:        "/foo",
	expectError: "non-absolute target URL",
}, {
	base:        "foo",
	expectError: "non-absolute base URL",
}, {
	base:        "/foo",
	target:      "foo",
	expectError: "non-absolute target URL",
}, {
	base:   "/foo",
	target: "/bar",
	expect: "bar",
}, {
	base:   "/foo/",
	target: "/bar",
	expect: "../bar",
}, {
	base:   "/bar",
	target: "/foo/",
	expect: "foo/",
}, {
	base:   "/foo/",
	target: "/bar/",
	expect: "../bar/",
}, {
	base:   "/foo/bar",
	target: "/bar/",
	expect: "../bar/",
}, {
	base:   "/foo/bar/",
	target: "/bar/",
	expect: "../../bar/",
}, {
	base:   "/foo/bar/baz",
	target: "/foo/targ",
	expect: "../targ",
}, {
	base:   "/foo/bar/baz/frob",
	target: "/foo/bar/one/two/",
	expect: "../one/two/",
}, {
	base:   "/foo/bar/baz/",
	target: "/foo/targ",
	expect: "../../targ",
}, {
	base:   "/foo/bar/baz/frob/",
	target: "/foo/bar/one/two/",
	expect: "../../one/two/",
}, {
	base:   "/foo/bar",
	target: "/foot/bar",
	expect: "../foot/bar",
}, {
	base:   "/foo/bar/baz/frob",
	target: "/foo/bar",
	expect: "../../bar",
}, {
	base:   "/foo/bar/baz/frob/",
	target: "/foo/bar",
	expect: "../../../bar",
}, {
	base:   "/foo/bar/baz/frob/",
	target: "/foo/bar/",
	expect: "../../",
}, {
	base:   "/foo/bar/baz",
	target: "/foo/bar/other",
	expect: "other",
}, {
	base:   "/foo/bar/",
	target: "/foo/bar/",
	expect: ".",
}, {
	base:   "/foo/bar",
	target: "/foo/bar",
	expect: "bar",
}, {
	base:   "/foo/bar/",
	target: "/foo/bar/",
	expect: ".",
}, {
	base:   "/foo/bar",
	target: "/foo/",
	expect: ".",
}, {
	base:   "/foo",
	target: "/",
	expect: ".",
}, {
	base:   "/foo/",
	target: "/",
	expect: "../",
}, {
	base:   "/foo/bar",
	target: "/",
	expect: "../",
}, {
	base:   "/foo/bar/",
	target: "/",
	expect: "../../",
}}

func (*utilSuite) TestRelativeURL(c *gc.C) {
	for i, test := range relativeURLTests {
		c.Logf("test %d: %q %q", i, test.base, test.target)
		// Check the test is valid.
		if test.expectError == "" {
			baseURL := &url.URL{Path: test.base}
			expectURL := &url.URL{Path: test.expect}
			targetURL := baseURL.ResolveReference(expectURL)
			c.Check(targetURL.Path, gc.Equals, test.target, gc.Commentf("resolve reference failure (%q + %q != %q)", test.base, test.expect, test.target))
		}

		result, err := router.RelativeURLPath(test.base, test.target)
		if test.expectError != "" {
			c.Assert(err, gc.ErrorMatches, test.expectError)
			c.Assert(result, gc.Equals, "")
		} else {
			c.Assert(err, gc.Equals, nil)
			c.Check(result, gc.Equals, test.expect)
		}
	}
}

type errorReader struct {
	err error
}

func (e errorReader) Read([]byte) (int, error) {
	return 0, e.err
}

var testErrorCause = errgo.New("expected error")

var unmarshalJSONResponseTests = []struct {
	about            string
	resp             *http.Response
	errorF           func(*http.Response) error
	expectValue      interface{}
	expectError      string
	expectErrorCause error
}{{
	about: "unmarshal object",
	resp: &http.Response{
		StatusCode: http.StatusOK,
		Header: http.Header{
			"Content-Type": {"application/json"},
		},
		Body: ioutil.NopCloser(strings.NewReader(`"OK"`)),
	},
	errorF: func(*http.Response) error {
		return errgo.New("unexpected error")
	},
	expectValue: "OK",
}, {
	about: "error response with function",
	resp: &http.Response{
		StatusCode: http.StatusBadRequest,
		Header: http.Header{
			"Content-Type": {"application/json"},
		},
		Body: ioutil.NopCloser(strings.NewReader(`"OK"`)),
	},
	errorF: func(*http.Response) error {
		return errgo.New("expected error")
	},
	expectError: "expected error",
}, {
	about: "error response without function",
	resp: &http.Response{
		StatusCode: http.StatusInternalServerError,
		Header: http.Header{
			"Content-Type": {"application/json"},
		},
		Body: ioutil.NopCloser(strings.NewReader(`"OK"`)),
	},
	expectValue: "OK",
}, {
	about: "unparsable content type",
	resp: &http.Response{
		StatusCode: http.StatusOK,
		Header: http.Header{
			"Content-Type": {"application/"},
		},
		Body: ioutil.NopCloser(strings.NewReader(`"OK"`)),
	},
	errorF: func(*http.Response) error {
		return errgo.New("expected error")
	},
	expectError: "cannot parse content type: mime: expected token after slash",
}, {
	about: "wrong content type",
	resp: &http.Response{
		StatusCode: http.StatusOK,
		Header: http.Header{
			"Content-Type": {"text/plain"},
		},
		Body: ioutil.NopCloser(strings.NewReader(`"OK"`)),
	},
	errorF: func(*http.Response) error {
		return errgo.New("expected error")
	},
	expectError: `unexpected content type "text/plain"`,
}, {
	about: "read error",
	resp: &http.Response{
		StatusCode: http.StatusOK,
		Header: http.Header{
			"Content-Type": {"application/json"},
		},
		Body: ioutil.NopCloser(errorReader{errgo.New("read error")}),
	},
	errorF: func(*http.Response) error {
		return errgo.New("unexpected error")
	},
	expectError: `cannot read response body: read error`,
}, {
	about: "read error",
	resp: &http.Response{
		StatusCode: http.StatusOK,
		Header: http.Header{
			"Content-Type": {"application/json"},
		},
		Body: ioutil.NopCloser(strings.NewReader(`"OK`)),
	},
	errorF: func(*http.Response) error {
		return errgo.New("unexpected error")
	},
	expectError: `cannot unmarshal response: unexpected end of JSON input`,
}, {
	about: "error with cause",
	resp: &http.Response{
		StatusCode: http.StatusBadRequest,
		Header: http.Header{
			"Content-Type": {"application/json"},
		},
		Body: ioutil.NopCloser(strings.NewReader(`"OK"`)),
	},
	errorF: func(*http.Response) error {
		return errgo.WithCausef(nil, testErrorCause, "an error message")
	},
	expectError:      "an error message",
	expectErrorCause: testErrorCause,
}}

func (*utilSuite) TestUnmarshalJSONObject(c *gc.C) {
	for i, test := range unmarshalJSONResponseTests {
		c.Logf("%d. %s", i, test.about)
		var v json.RawMessage
		err := router.UnmarshalJSONResponse(test.resp, &v, test.errorF)
		if test.expectError != "" {
			c.Assert(err, gc.ErrorMatches, test.expectError)
			if test.expectErrorCause != nil {
				c.Assert(errgo.Cause(err), gc.Equals, test.expectErrorCause)
			}
			continue
		}
		c.Assert(err, gc.Equals, nil)
		c.Assert(string(v), jc.JSONEquals, test.expectValue)
	}
}
