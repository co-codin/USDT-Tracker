// Package metrics exposes Prometheus metrics for the listener.
package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const ns = "tron_listener"

// Metrics holds all collectors. It implements tron.Observer and sink.Recorder.
type Metrics struct {
	Registry *prometheus.Registry

	HeadBlock         prometheus.Gauge
	SafeHeadBlock     prometheus.Gauge
	CursorBlock       prometheus.Gauge
	LagBlocks         prometheus.Gauge
	BlocksProcessed   prometheus.Counter
	TransfersDecoded  prometheus.Counter
	TransfersMatched  *prometheus.CounterVec
	DecodeErrors      prometheus.Counter
	BlockErrors       prometheus.Counter
	DispatchRetries   prometheus.Counter
	RPCRequests       *prometheus.CounterVec
	RPCDuration       *prometheus.HistogramVec
	RPCRetries        *prometheus.CounterVec
	SinkDeliveries    *prometheus.CounterVec
	SinkTransfers     *prometheus.CounterVec
	SinkDuration      *prometheus.HistogramVec
	LastProcessedTime prometheus.Gauge
}

// New creates and registers all metrics on a fresh registry.
func New() *Metrics {
	r := prometheus.NewRegistry()
	r.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	f := func(c prometheus.Collector) { r.MustRegister(c) }
	m := &Metrics{Registry: r}

	m.HeadBlock = prometheus.NewGauge(prometheus.GaugeOpts{Namespace: ns, Name: "head_block", Help: "Latest block number reported by the node."})
	m.SafeHeadBlock = prometheus.NewGauge(prometheus.GaugeOpts{Namespace: ns, Name: "safe_head_block", Help: "Head minus configured confirmations."})
	m.CursorBlock = prometheus.NewGauge(prometheus.GaugeOpts{Namespace: ns, Name: "cursor_block", Help: "Last fully processed block."})
	m.LagBlocks = prometheus.NewGauge(prometheus.GaugeOpts{Namespace: ns, Name: "lag_blocks", Help: "Safe head minus cursor."})
	m.LastProcessedTime = prometheus.NewGauge(prometheus.GaugeOpts{Namespace: ns, Name: "last_processed_timestamp_seconds", Help: "Unix time when the last block was processed."})
	m.BlocksProcessed = prometheus.NewCounter(prometheus.CounterOpts{Namespace: ns, Name: "blocks_processed_total", Help: "Blocks fully processed."})
	m.TransfersDecoded = prometheus.NewCounter(prometheus.CounterOpts{Namespace: ns, Name: "transfers_decoded_total", Help: "Token Transfer events decoded (before filtering)."})
	m.TransfersMatched = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "transfers_matched_total", Help: "Transfers matched by the filter, by reason."}, []string{"reason"})
	m.DecodeErrors = prometheus.NewCounter(prometheus.CounterOpts{Namespace: ns, Name: "decode_errors_total", Help: "Malformed token logs skipped."})
	m.BlockErrors = prometheus.NewCounter(prometheus.CounterOpts{Namespace: ns, Name: "block_fetch_errors_total", Help: "Block fetches that failed after retries."})
	m.DispatchRetries = prometheus.NewCounter(prometheus.CounterOpts{Namespace: ns, Name: "dispatch_retries_total", Help: "Block deliveries retried because a required sink failed."})
	m.RPCRequests = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "rpc_requests_total", Help: "TRON API requests by endpoint and status."}, []string{"endpoint", "status"})
	m.RPCDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: ns, Name: "rpc_request_duration_seconds", Help: "TRON API request latency.", Buckets: prometheus.DefBuckets}, []string{"endpoint"})
	m.RPCRetries = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "rpc_retries_total", Help: "TRON API retries by endpoint and reason."}, []string{"endpoint", "reason"})
	m.SinkDeliveries = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "sink_deliveries_total", Help: "Batch deliveries by sink and status (ok|error|dropped)."}, []string{"sink", "status"})
	m.SinkTransfers = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "sink_transfers_total", Help: "Transfers delivered by sink and status."}, []string{"sink", "status"})
	m.SinkDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: ns, Name: "sink_delivery_duration_seconds", Help: "Sink delivery latency.", Buckets: prometheus.DefBuckets}, []string{"sink"})

	for _, c := range []prometheus.Collector{m.HeadBlock, m.SafeHeadBlock, m.CursorBlock, m.LagBlocks, m.LastProcessedTime,
		m.BlocksProcessed, m.TransfersDecoded, m.TransfersMatched, m.DecodeErrors, m.BlockErrors, m.DispatchRetries,
		m.RPCRequests, m.RPCDuration, m.RPCRetries, m.SinkDeliveries, m.SinkTransfers, m.SinkDuration} {
		f(c)
	}
	return m
}

// Handler returns the /metrics HTTP handler.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{Registry: m.Registry})
}

// ObserveRequest implements tron.Observer.
func (m *Metrics) ObserveRequest(endpoint, status string, d time.Duration) {
	m.RPCRequests.WithLabelValues(endpoint, status).Inc()
	m.RPCDuration.WithLabelValues(endpoint).Observe(d.Seconds())
}

// ObserveRetry implements tron.Observer.
func (m *Metrics) ObserveRetry(endpoint, reason string) {
	m.RPCRetries.WithLabelValues(endpoint, reason).Inc()
}

// ObserveSink implements sink.Recorder.
func (m *Metrics) ObserveSink(sink, status string, n int, d time.Duration) {
	m.SinkDeliveries.WithLabelValues(sink, status).Inc()
	m.SinkTransfers.WithLabelValues(sink, status).Add(float64(n))
	m.SinkDuration.WithLabelValues(sink).Observe(d.Seconds())
}
