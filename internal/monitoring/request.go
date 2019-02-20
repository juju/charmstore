// Copyright 2016 Canonical Ltd.

package monitoring // import "gopkg.in/juju/charmstore.v5/internal/monitoring"

import (
	"fmt"
	"net/http"
	"time"
)

// Request represents a monitoring request. To record a request, either
// create a new request with NewRequest or call Reset on an existing
// Request; then call Done when the request has completed.
type Request struct {
	startTime time.Time
	root      string
	endpoint  string
	request   *http.Request
}

// NewRequest returns a new monitoring request
// for monitoring a request within the given root.
// When the request is done, Done should be called.
func NewRequest(req *http.Request, root string) *Request {
	var r Request
	r.Reset(req, root)
	return &r
}

// Reset resets r to indicate that a new request has started. The
// parameter holds the API root (for example the API version).
func (r *Request) Reset(req *http.Request, root string) {
	r.startTime = time.Now()
	r.request = req
	r.endpoint = ""
	r.root = root
}

// SetEndpoint sets the endpoint of the request.
func (r *Request) SetEndpoint(endpoint string) {
	r.endpoint = endpoint
}

// Endpoint returns the currently set endpoint for the request. If it is not set
// it falls back to the request URL path if one exists (e.g. it may not in unit testing).
func (r *Request) Endpoint() string {
	if r.endpoint == "" && r.request != nil {
		return r.request.URL.Path
	}
	return r.endpoint
}

// Done records that the request is complete, and records any metrics for the request since the last call to Reset.
// If the request endpoint is empty, it falls back to the path from the request url.
func (r *Request) Done(status func() int) {
	statusStr := fmt.Sprint(status())
	requestDuration.WithLabelValues(r.request.Method, r.root, statusStr).Observe(float64(time.Since(r.startTime)) / float64(time.Second))
}
