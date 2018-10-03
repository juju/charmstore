// Copyright 2016 Canonical Ltd.

package monitoring // import "gopkg.in/juju/charmstore.v5/internal/monitoring"

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Duration represents a time duration to be monitored.
// The duration starts when the Duration is created and finishes when Done is called.
type Duration struct {
	metric    prometheus.Summary
	startTime time.Time
}

// DurationVec represents a time duration to be monitored, but uses a
// prometheus.SummaryVec collector to allow dimension partitioning with labels.
// The duration starts when the Duration is created and finishes when Done is called.
type DurationVec struct {
	metric      *prometheus.SummaryVec
	labelValues []string
	startTime   time.Time
}

func newDuration(metric prometheus.Summary) *Duration {
	return &Duration{
		metric:    metric,
		startTime: time.Now(),
	}
}

func newDurationVec(metric *prometheus.SummaryVec, labelValues ...string) *DurationVec {
	return &DurationVec{
		metric:      metric,
		startTime:   time.Now(),
		labelValues: labelValues,
	}
}

// NewUploadProcessingDuration returns a new Duration to be used for measuring
// the time taken to process an upload.
func NewUploadProcessingDuration() *Duration {
	return newDuration(uploadProcessingDuration)
}

// NewMetaDuration returns a new DurationVec to be used for measuring the time
// taken to get metadata for an entity.
func NewMetaDuration(metaName string) *DurationVec {
	return newDurationVec(metaDuration, metaName)
}

// NewBlobstoreGCDuration returns a new Duration to be used for measuring the
// time taken to run the blobstore garbage collector.
func NewBlobstoreGCDuration() *Duration {
	return newDuration(blobstoreGCDuration)
}

// Done observes the duration on a Duration as a metric.
// It should only be called once.
func (d *Duration) Done() {
	d.metric.Observe(float64(time.Since(d.startTime)) / float64(time.Second))
}

// Done observes the duration on a DurationVec as a metric.
// It should only be called once.
func (d *DurationVec) Done() {
	d.metric.WithLabelValues(d.labelValues...).Observe(float64(time.Since(d.startTime)) / float64(time.Second))
}
