// Copyright 2018 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package dockerauth

import (
	"fmt"
	"strings"

	errgo "gopkg.in/errgo.v1"
)

// parseResourceAccess parses the requested access for a single resource
// from a scope. This is parsed as a resourcescope from the grammer
// specified in
// https://docs.docker.com/registry/spec/auth/scope/#resource-scope-grammar.
func parseResourceAccessRights(s string) (ResourceAccessRights, error) {
	var ra ResourceAccessRights
	i := strings.IndexByte(s, ':')
	j := strings.LastIndexByte(s, ':')
	if i == j {
		return ra, errgo.Newf("invalid resource scope %q", s)
	}
	ra.Type = s[:i]
	ra.Name = s[i+1 : j]
	actions := s[j+1:]
	if actions != "" {
		ra.Actions = strings.Split(actions, ",")
	}
	return ra, nil
}

// ParseScope parses a requested scope and returns the set or requested
// resource accesses. An error of type *ScopeParseError is returned if
// any part of the scope is not valid. Any valid resource scopes are
// always returned.
func ParseScope(s string) ([]ResourceAccessRights, error) {
	if s == "" {
		return nil, nil
	}
	var errs []error
	var ras []ResourceAccessRights
	for _, s := range strings.Split(s, " ") {
		ra, err := parseResourceAccessRights(s)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		ras = append(ras, ra)
	}
	if len(errs) > 0 {
		return ras, &ScopeParseError{errs}
	}
	return ras, nil
}

// A ScopeParseError is an error returned when part of a scope is not
// valid.
type ScopeParseError struct {
	Errs []error
}

// Error implements error.
func (e *ScopeParseError) Error() string {
	if len(e.Errs) == 1 {
		return e.Errs[0].Error()
	}
	errs := make([]string, len(e.Errs))
	for i, err := range e.Errs {
		errs[i] = err.Error()
	}
	return fmt.Sprintf("[%s]", strings.Join(errs, ", "))
}
