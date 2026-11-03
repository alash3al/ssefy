// Package metrics is the Prometheus surface of ssefy.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	HTTPRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ssefy_http_requests_total",
		Help: "HTTP requests by method, route and status.",
	}, []string{"method", "route", "status"})

	HTTPDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ssefy_http_duration_seconds",
		Help:    "Request handling time. For a stream this is the life of the connection.",
		Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 30, 300, 3600},
	}, []string{"method", "route"})

	Readers = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "ssefy_readers",
		Help: "Open SSE connections on this node.",
	})

	Appended = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ssefy_events_appended_total",
		Help: "Events accepted from producers.",
	}, []string{"kind"})

	Delivered = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ssefy_events_delivered_total",
		Help: "Event frames written to readers.",
	}, []string{"kind"})

	DeliveryLag = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "ssefy_event_delivery_lag_seconds",
		Help:    "Time from commit to delivery for events sent after the initial replay.",
		Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 30},
	})

	Notifications = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ssefy_notifications_total",
		Help: "LISTEN/NOTIFY messages received by this node.",
	}, []string{"kind"})

	ListenerUp = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "ssefy_listener_up",
		Help: "1 while this node holds its LISTEN connection.",
	})

	ListenerReconnects = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "ssefy_listener_reconnects_total",
		Help: "LISTEN connection failures and reconnects.",
	})

	SlowReaders = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "ssefy_slow_readers_total",
		Help: "Readers disconnected because they fell behind a stored wake-up.",
	})

	TransientDropped = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "ssefy_transient_dropped_total",
		Help: "Transient events dropped because a reader was behind.",
	})
)

func init() {
	prometheus.MustRegister(
		HTTPRequests, HTTPDuration,
		Readers, Appended, Delivered, DeliveryLag,
		Notifications, ListenerUp, ListenerReconnects,
		SlowReaders, TransientDropped,
	)
}

// Handler serves the default registry in Prometheus text format.
func Handler() http.Handler {
	return promhttp.Handler()
}
