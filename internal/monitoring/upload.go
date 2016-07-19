// Copyright 2016 Canonical Ltd.

package monitoring

import (
	"time"
)

// UploadProcessingDuration represents a monitoring duration.
type UploadProcessingDuration struct {
	startTime time.Time
}

// Return a new UploadProcessingDuration with its start time set to now.
func NewUploadProcessingDuration() *UploadProcessingDuration {
	return &UploadProcessingDuration{
		startTime: time.Now(),
	}
}

// ObserveMetric observes this metric.
func (r *UploadProcessingDuration) ObserveMetric() {
	uploadProcessingDuration.Observe(float64(time.Since(r.startTime)) / float64(time.Microsecond))
}
