// Copyright 2018 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package dockerauth

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"net/http"
	"strings"
	"time"

	jwt "github.com/dgrijalva/jwt-go"
	"github.com/juju/loggo"
	"github.com/julienschmidt/httprouter"
	"golang.org/x/net/context"
	errgo "gopkg.in/errgo.v1"
	httprequest "gopkg.in/httprequest.v1"
	"gopkg.in/juju/charmstore.v5/internal/charmstore"
)

var logger = loggo.GetLogger("charmstore.internal.dockerauth")

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

type Handler struct {
	Key        crypto.Signer
	Certs      []*x509.Certificate
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
	Access []ResourceAccessRights `json:"access"`
}

func (h *Handler) Token(p httprequest.Params, req *tokenRequest) (*tokenResponse, error) {
	ras, err := ParseScope(req.Scope)
	if err != nil {
		logger.Infof("Invalid scope: %s", err)
	}
	var issuer string
	if len(h.Certs) > 0 {
		issuer = h.Certs[0].Subject.CommonName
	}
	issuedAt := time.Now()
	expiresAt := issuedAt.Add(h.TokenValid)
	claims := dockerRegistryClaims{
		StandardClaims: jwt.StandardClaims{
			Audience:  req.Service,
			ExpiresAt: expiresAt.Unix(),
			IssuedAt:  issuedAt.Unix(),
			NotBefore: issuedAt.Unix(),
			Issuer:    issuer,
		},
		Access: ras,
	}
	var sm jwt.SigningMethod
	switch h.Key.(type) {
	case *ecdsa.PrivateKey:
		sm = jwt.SigningMethodES256
	case *rsa.PrivateKey:
		sm = jwt.SigningMethodRS256
	default:
		sm = jwt.SigningMethodNone
	}
	tok := jwt.NewWithClaims(sm, claims)
	certs := make([]string, len(h.Certs))
	for i, c := range h.Certs {
		certs[i] = base64.StdEncoding.EncodeToString(c.Raw)
	}
	// The x5c header contains the certificate chain used by the
	// docker-registry to authenticate the token.
	tok.Header["x5c"] = certs
	s, err := tok.SignedString(h.Key)
	if err != nil {
		return nil, errgo.Mask(err)
	}
	return &tokenResponse{
		Token:     s,
		ExpiresIn: int(h.TokenValid / time.Second),
		IssuedAt:  issuedAt,
	}, nil
}

func (h *Handler) handler(p httprequest.Params) (*Handler, context.Context, error) {
	return h, p.Context, nil
}

func NewAPIHandler(p charmstore.APIHandlerParams) (charmstore.HTTPCloseHandler, error) {
	logger.Infof("Adding docker-registry")
	h := &Handler{
		Key:        p.DockerRegistryAuthKey,
		Certs:      p.DockerRegistryAuthCertificates,
		TokenValid: 120 * time.Second,
	}
	r := httprouter.New()
	srv := httprequest.Server{
		ErrorMapper: errorMapper,
	}
	httprequest.AddHandlers(r, srv.Handlers(func(p httprequest.Params) (*Handler, context.Context, error) {
		return h, p.Context, nil
	}))
	return server{
		Handler: r,
	}, nil
}

type server struct {
	http.Handler
}

func (server) Close() {}

func errorMapper(ctx context.Context, err error) (httpStatus int, errorBody interface{}) {
	return http.StatusInternalServerError, &httprequest.RemoteError{
		Message: err.Error(),
	}
}
