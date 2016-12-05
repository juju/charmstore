// Copyright 2016 Canonical Ltd.

package monitoring

import (
	"time"
)

// Request represents a monitoring request.
type Request struct {
	startTime time.Time
	label     string
}

// Reset the monitor start time to now and the label to blank.
func (r *Request) Reset() {
	r.startTime = time.Now()
	r.label = ""
}

// AppendLabel appends the given label value to the label of the monitor.
// This supports piecing together parameterized routes as labels.
func (r *Request) AppendLabel(label string) {
	r.label += label
}

// ObserveMetric observes this metric.
func (r *Request) ObserveMetric() {
	// TODO reenable this when the problem with inappropriate use of
	// AppendLabel has been fixed.
	// For example, we're seeing a path of /v4/meta/:meta/any/v4/:meta/any/v4/:meta/any/v4/:meta/any/v4/:meta/any/v4/:meta/any
	// for some requests.
	// requestDuration.WithLabelValues(r.label).Observe(float64(time.Since(r.startTime)) / float64(time.Second))
}

// Label returns unexported label for testing.
func (r *Request) Label() string { return r.label }
