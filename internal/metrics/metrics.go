// Package metrics owns the Prometheus collectors and the /metrics handler.
// A single dedicated registry is used so the exposed series are exactly the
// ones defined in PROJECT_BRIEF.md §8.5.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics bundles every relay collector and the /metrics handler.
type Metrics struct {
	EventsReceived *prometheus.CounterVec
	EventsRejected *prometheus.CounterVec
	Deliveries     *prometheus.CounterVec
	Attempts       prometheus.Counter
	Latency        prometheus.Histogram
	Due            prometheus.Gauge
	LastSuccess    prometheus.Gauge
	WorkerClaims   prometheus.Counter
	Handler        http.Handler
}

// New registers all collectors on a fresh registry.
func New() *Metrics {
	r := prometheus.NewRegistry()
	m := &Metrics{
		EventsReceived: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "relay_events_received_total",
			Help: "Producer events accepted by POST /v1/webhooks.",
		}, []string{"source"}),
		EventsRejected: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "relay_events_rejected_total",
			Help: "Requests rejected by the ingest API, by reason.",
		}, []string{"reason"}),
		Deliveries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "relay_deliveries_total",
			Help: "Deliveries by terminal or recovery status.",
		}, []string{"status"}),
		Attempts: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "relay_delivery_attempts_total",
			Help: "Delivery attempts made (every claim, success or failure).",
		}),
		Latency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "relay_delivery_latency_seconds",
			Help:    "Latency of a single delivery attempt, per attempt outcome.",
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		}),
		Due: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "relay_deliveries_due",
			Help: "Pending deliveries whose next_attempt_at has passed; sampled each claim loop.",
		}),
		LastSuccess: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "relay_last_success_timestamp_seconds",
			Help: "Unix time of the last successful delivery; 0 if none yet.",
		}),
		WorkerClaims: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "relay_worker_claims_total",
			Help: "Deliveries claimed by the worker.",
		}),
	}
	r.MustRegister(m.EventsReceived, m.EventsRejected, m.Deliveries, m.Attempts, m.Latency, m.Due, m.LastSuccess, m.WorkerClaims)
	m.Handler = promhttp.HandlerFor(r, promhttp.HandlerOpts{})
	return m
}
