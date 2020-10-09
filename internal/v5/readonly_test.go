// Copyright 2018 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package v5_test

import (
	"net/http"
	"strings"

	"github.com/juju/charmrepo/v6/csclient/params"
	"github.com/juju/testing/httptesting"
	gc "gopkg.in/check.v1"
	"gopkg.in/juju/charmstore.v5/internal/storetesting"
)

type ReadOnlySuite struct {
	commonSuite
}

var _ = gc.Suite(&ReadOnlySuite{})

func (s *ReadOnlySuite) SetUpSuite(c *gc.C) {
	s.readOnly = true
	s.enableIdentity = true
	s.commonSuite.SetUpSuite(c)
}

func (s *ReadOnlySuite) TestUploadArchiveFails(c *gc.C) {
	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler: s.srv,
		URL:     storeURL("~bob/wordpress") + "/archive",
		Method:  "POST",
		Header: http.Header{
			"Content-Type": {"application/zip"},
		},
		Body:         strings.NewReader(`ignored`),
		Username:     testUsername,
		Password:     testPassword,
		ExpectStatus: http.StatusForbidden,
		ExpectBody: params.Error{
			Message: params.ErrReadOnly.Error(),
			Code:    params.ErrReadOnly,
		},
	})
}

func (s *ReadOnlySuite) TestSetPermFails(c *gc.C) {
	id := newResolvedURL("~charmers/trusty/wordpress-1", 1)
	err := s.store.AddCharmWithArchive(id, storetesting.NewCharm(nil))
	c.Assert(err, gc.Equals, nil)
	s.doAsUser("charmers", func() {
		httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
			Handler: s.srv,
			Do:      bakeryDo(nil),
			Method:  "PUT",
			JSONBody: params.PermRequest{
				Read:  []string{"foo"},
				Write: []string{"bar"},
			},
			URL:          storeURL("~charmers/trusty/wordpress-1/meta/perm?channel=edge"),
			ExpectStatus: http.StatusForbidden,
			ExpectBody: params.Error{
				Message: params.ErrReadOnly.Error(),
				Code:    params.ErrReadOnly,
			},
		})
	})
}

func (s *ReadOnlySuite) TestGetSucceeds(c *gc.C) {
	id := newResolvedURL("~charmers/trusty/wordpress-1", 1)
	err := s.store.AddCharmWithArchive(id, storetesting.NewCharm(nil))
	c.Assert(err, gc.Equals, nil)
	s.doAsUser("charmers", func() {
		s.assertGet(c, "wordpress/meta/perm?channel=unpublished", params.PermResponse{
			Read:  []string{"charmers"},
			Write: []string{"charmers"},
		})
	})
}
