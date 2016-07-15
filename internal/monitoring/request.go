// Copyright 2016 Canonical Ltd.

package monitoring

import (
	"net/url"
	"time"
)

// Request represents a monitoring request.
type Request struct {
	startTime time.Time
	label     string
}

// Reset the request monitor.
func (r *Request) Reset(url *url.URL) {
	r.label = url.Path
	r.startTime = time.Now()
}

// ObserveMetric observes this metric.
func (r *Request) ObserveMetric() {
	requestDuration.WithLabelValues(r.label).Observe(float64(time.Since(r.startTime)) / float64(time.Microsecond))
}
