// Copyright 2018 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package dockerauth

import (
	"crypto/ecdsa"
	"crypto/rsa"
	"encoding/base64"
	"fmt"
	"net/http"
	"strings"
	"time"

	jwt "github.com/dgrijalva/jwt-go"
	"github.com/juju/idmclient"
	"github.com/juju/loggo"
	"github.com/julienschmidt/httprouter"
	"golang.org/x/net/context"
	errgo "gopkg.in/errgo.v1"
	httprequest "gopkg.in/httprequest.v1"
	charm "gopkg.in/juju/charm.v6"
	"gopkg.in/juju/charmrepo.v3/csclient/params"
	"gopkg.in/macaroon-bakery.v2-unstable/bakery/checkers"
	"gopkg.in/macaroon.v2-unstable"

	"gopkg.in/juju/charmstore.v5/internal/charmstore"
	"gopkg.in/juju/charmstore.v5/internal/mongodoc"
)

var logger = loggo.GetLogger("charmstore.internal.dockerauth")

// parseResourceAccess parses the requested access for a single resource
// from a scope. This is parsed as a resourcescope from the grammer
// specified in
// https://docs.docker.com/registry/spec/auth/scope/#resource-scope-grammar.
func parseResourceAccessRights(s string) (resourceAccessRights, error) {
	var ra resourceAccessRights
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

// parseScope parses a requested scope and returns the set or requested
// resource accesses. An error of type *ScopeParseError is returned if
// any part of the scope is not valid. Any valid resource scopes are
// always returned.
func parseScope(s string) ([]resourceAccessRights, error) {
	if s == "" {
		return nil, nil
	}
	var errs []error
	var ras []resourceAccessRights
	for _, s := range strings.Split(s, " ") {
		ra, err := parseResourceAccessRights(s)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		ras = append(ras, ra)
	}
	if len(errs) > 0 {
		return ras, &scopeParseError{errs}
	}
	return ras, nil
}

// A scopeParseError is an error returned when part of a scope is not
// valid.
type scopeParseError struct {
	errs []error
}

// Error implements error.
func (e *scopeParseError) Error() string {
	if len(e.errs) == 1 {
		return e.errs[0].Error()
	}
	errs := make([]string, len(e.errs))
	for i, err := range e.errs {
		errs[i] = err.Error()
	}
	return fmt.Sprintf("[%s]", strings.Join(errs, ", "))
}

type Handler struct {
	params     charmstore.APIHandlerParams
	TokenValid time.Duration
}

type tokenRequest struct {
	httprequest.Route `httprequest:"GET /token"`
	Scope             string `httprequest:"scope,form"`
	Service           string `httprequest:"service,form"`
}

type tokenResponse struct {
	Token     string    `json:"token"`
	ExpiresIn int       `json:"expires_in"`
	IssuedAt  time.Time `json:"issued_at"`
}

type dockerRegistryClaims struct {
	jwt.StandardClaims
	Access []resourceAccessRights `json:"access"`
}

func (h *Handler) handler(p httprequest.Params) (*handler, context.Context, error) {
	store, err := h.params.Pool.RequestStore()
	if err != nil {
		return nil, p.Context, errgo.Mask(err)
	}
	return &handler{h, store}, p.Context, nil
}

type handler struct {
	h     *Handler
	store *charmstore.Store
}

func (h *handler) Close() error {
	h.store.Close()
	return nil
}

// Token implements the token issuing endpoint for a docker registry
// authorization service. See
// https://docs.docker.com/registry/spec/auth/token/ for the protocol
// details.
func (h *handler) Token(p httprequest.Params, req *tokenRequest) (*tokenResponse, error) {
	ras, err := parseScope(req.Scope)
	if err != nil {
		return nil, errgo.Mask(err)
	}
	ms := credentials(p.Request)
	declared := checkers.InferDeclared(ms)
	// TODO(mhilton) support the "everyone" ACL entry to allow
	// unauthenticated access.
	var user user = noUser{}
	if identity, err := h.h.params.IDMClient.DeclaredIdentity(declared); err == nil {
		if u, ok := identity.(*idmclient.User); ok {
			user = u
		}
	}
	filteredRAs := make([]resourceAccessRights, 0, len(ras))
	for _, ra := range ras {
		if ra.Type != "repository" {
			continue
		}
		acl, err := h.repositoryACL(ra.Name)
		if err != nil {
			return nil, errgo.Mask(err)
		}
		filteredActions := make([]string, 0, len(ra.Actions))
		for _, a := range ra.Actions {
			switch a {
			case "pull":
				allow, _ := user.Allow(acl.Read)
				if !allow {
					continue
				}
			case "push":
				allow, _ := user.Allow(acl.Write)
				if !allow {
					continue
				}
			default:
				continue
			}
			// TODO(mhilton) support more caveats.
			if h.store.Bakery.Check(ms, checkers.New(declared)) != nil {
				continue
			}
			filteredActions = append(filteredActions, a)
		}
		if len(filteredActions) == 0 {
			continue
		}
		filteredRAs = append(filteredRAs, resourceAccessRights{
			Type:    ra.Type,
			Name:    ra.Name,
			Actions: filteredActions,
		})
	}
	issuedAt := time.Now()
	s, err := h.createToken(filteredRAs, req.Service, issuedAt)
	if err != nil {
		return nil, errgo.Mask(err)
	}
	return &tokenResponse{
		Token:     s,
		ExpiresIn: int(h.h.TokenValid / time.Second),
		IssuedAt:  issuedAt,
	}, nil
}

func credentials(req *http.Request) macaroon.Slice {
	_, pw, _ := req.BasicAuth()
	b, err := base64.RawStdEncoding.DecodeString(pw)
	if err != nil {
		logger.Debugf("invalid macaroon: %s", err)
		return nil
	}
	var ms macaroon.Slice
	if err := ms.UnmarshalBinary(b); err != nil {
		logger.Debugf("invalid macaroon: %s", err)
		return nil
	}
	return ms
}

// repositoryACL finds the ACL corresponding to the given
// docker-repository name. A repository name is of the form
// <owner>/<charm name>/<channel>/<resource name>. If an invalid name is
// given then an empty ACL will be returned causing the access request to
// be denied.
func (h *handler) repositoryACL(name string) (mongodoc.ACL, error) {
	parts := strings.SplitN(name, "/", 4)
	if len(parts) != 4 {
		return mongodoc.ACL{}, nil
	}
	baseURL := charm.URL{
		Schema:   "cs",
		User:     parts[0],
		Name:     parts[1],
		Revision: -1,
	}
	be, err := h.store.FindBaseEntity(&baseURL, map[string]int{
		"_id":         1,
		"channelacls": 1,
	})
	if err != nil {
		return mongodoc.ACL{}, errgo.Mask(err)
	}
	// TODO(mhilton) validate the resource name corresponds to a charm resource?
	return be.ChannelACLs[params.Channel(parts[2])], nil
}

// createToken creates a JWT for the given service with the givne access
// rights.
func (h *handler) createToken(ras []resourceAccessRights, service string, issuedAt time.Time) (string, error) {
	var issuer string
	if len(h.h.params.DockerRegistryAuthCertificates) > 0 {
		issuer = h.h.params.DockerRegistryAuthCertificates[0].Subject.CommonName
	}
	expiresAt := issuedAt.Add(h.h.TokenValid)
	claims := dockerRegistryClaims{
		StandardClaims: jwt.StandardClaims{
			Audience:  service,
			ExpiresAt: expiresAt.Unix(),
			IssuedAt:  issuedAt.Unix(),
			NotBefore: issuedAt.Unix(),
			Issuer:    issuer,
		},
		Access: ras,
	}
	var sm jwt.SigningMethod
	switch h.h.params.DockerRegistryAuthKey.(type) {
	case *ecdsa.PrivateKey:
		sm = jwt.SigningMethodES256
	case *rsa.PrivateKey:
		sm = jwt.SigningMethodRS256
	default:
		sm = jwt.SigningMethodNone
	}
	tok := jwt.NewWithClaims(sm, claims)
	certs := make([]string, len(h.h.params.DockerRegistryAuthCertificates))
	for i, c := range h.h.params.DockerRegistryAuthCertificates {
		certs[i] = base64.StdEncoding.EncodeToString(c.Raw)
	}
	// The x5c header contains the certificate chain used by the
	// docker-registry to authenticate the token.
	tok.Header["x5c"] = certs
	s, err := tok.SignedString(h.h.params.DockerRegistryAuthKey)
	if err != nil {
		return "", errgo.Mask(err)
	}
	return s, nil
}

func NewAPIHandler(p charmstore.APIHandlerParams) (charmstore.HTTPCloseHandler, error) {
	logger.Infof("Adding docker-registry")
	h := &Handler{
		params: p,
		// TODO(mhilton) make TokenValid configurable.
		TokenValid: 120 * time.Second,
	}
	r := httprouter.New()
	srv := httprequest.Server{
		ErrorMapper: errorMapper,
	}
	httprequest.AddHandlers(r, srv.Handlers(h.handler))
	return server{
		Handler: r,
	}, nil
}

type server struct {
	http.Handler
}

func (server) Close() {}

func errorMapper(ctx context.Context, err error) (httpStatus int, errorBody interface{}) {
	// TODO return docker-registry standard error format (see
	// https://docs.docker.com/registry/spec/api/#errors)
	return http.StatusInternalServerError, &httprequest.RemoteError{
		Message: err.Error(),
	}
}

type user interface {
	Allow([]string) (bool, error)
}

type noUser struct {
}

func (noUser) Allow([]string) (bool, error) {
	return false, nil
}
