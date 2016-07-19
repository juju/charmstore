// Copyright 2016 Canonical Ltd.

package monitoring

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	requestDuration = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace: "charmstore",
		Subsystem: "handler",
		Name:      "request_duration",
		Help:      "The duration of a web request in seconds.",
	}, []string{"path_pattern"})
	uploadProcessingDuration = prometheus.NewSummary(prometheus.SummaryOpts{
		Namespace: "charmstore",
		Subsystem: "archive",
		Name:      "processing_duration",
		Help:      "The processing duration of a charm upload in seconds.",
	})
)

func init() {
	prometheus.MustRegister(requestDuration)
	prometheus.MustRegister(uploadProcessingDuration)
}
