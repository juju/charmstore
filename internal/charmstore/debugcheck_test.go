// Copyright 2015 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package charmstore

import (
	"errors"
	"net/http"

	"github.com/juju/httprequest"
	"github.com/juju/testing/httptesting"
	"github.com/julienschmidt/httprouter"
	gc "gopkg.in/check.v1"
	"gopkg.in/juju/charmrepo.v2-unstable/csclient/params"

	"gopkg.in/juju/charmstore.v5-unstable/internal/router"
)

type debugCheckSuite struct{}

var _ = gc.Suite(&debugCheckSuite{})

var debugCheckTests = []struct {
	about        string
	checks       map[string]func() error
	expectStatus int
	expectBody   interface{}
}{{
	about:        "no checks",
	expectStatus: http.StatusOK,
	expectBody:   map[string]string{},
}, {
	about: "passing check",
	checks: map[string]func() error{
		"pass": func() error { return nil },
	},
	expectStatus: http.StatusOK,
	expectBody: map[string]string{
		"pass": "OK",
	},
}, {
	about: "failing check",
	checks: map[string]func() error{
		"fail": func() error { return errors.New("test fail") },
	},
	expectStatus: http.StatusInternalServerError,
	expectBody: params.Error{
		Message: "check failure: [fail: test fail]",
	},
}, {
	about: "many pass",
	checks: map[string]func() error{
		"pass1": func() error { return nil },
		"pass2": func() error { return nil },
	},
	expectStatus: http.StatusOK,
	expectBody: map[string]string{
		"pass1": "OK",
		"pass2": "OK",
	},
}, {
	about: "many fail",
	checks: map[string]func() error{
		"fail1": func() error { return errors.New("test fail1") },
		"fail2": func() error { return errors.New("test fail2") },
	},
	expectStatus: http.StatusInternalServerError,
	expectBody: params.Error{
		Message: "check failure: [fail1: test fail1] [fail2: test fail2]",
	},
}, {
	about: "pass and fail",
	checks: map[string]func() error{
		"pass": func() error { return nil },
		"fail": func() error { return errors.New("test fail") },
	},
	expectStatus: http.StatusInternalServerError,
	expectBody: params.Error{
		Message: "check failure: [fail: test fail] [pass: OK]",
	},
}}

func (s *debugCheckSuite) TestDebugCheck(c *gc.C) {
	for i, test := range debugCheckTests {
		c.Logf("%d. %s", i, test.about)
		hnd := &debugReqHandler{
			h: &debugHandler{
				checks: test.checks,
			},
		}
		httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
			Handler:      handler(hnd),
			URL:          "/debug/check",
			ExpectStatus: test.expectStatus,
			ExpectBody:   test.expectBody,
		})
	}
}

func handler(hnd *debugReqHandler) http.Handler {
	f := func(p httprequest.Params) (*debugReqHandler, error) {
		return hnd, nil
	}
	r := httprouter.New()
	for _, h := range router.ErrorToResp.Handlers(f) {
		r.Handle(h.Method, h.Path, h.Handle)
	}
	return r
}
