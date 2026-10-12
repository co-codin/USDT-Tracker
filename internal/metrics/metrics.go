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

// Metrics holds all collectors. It implements sink.Recorder; For(chain)
// returns a per-chain view that implements the chain clients' Observer.
type Metrics struct {
	Registry *prometheus.Registry

	// Per-chain collectors (label "chain": tron|ton|eth). Use For(chain) to get
	// the curried view a listener writes to.
	HeadBlock         *prometheus.GaugeVec
	SafeHeadBlock     *prometheus.GaugeVec
	CursorBlock       *prometheus.GaugeVec
	LagBlocks         *prometheus.GaugeVec
	BlocksProcessed   *prometheus.CounterVec
	TransfersDecoded  *prometheus.CounterVec
	TransfersMatched  *prometheus.CounterVec // chain, reason
	DecodeErrors      *prometheus.CounterVec
	BlockErrors       *prometheus.CounterVec
	DispatchRetries   *prometheus.CounterVec
	RPCRequests       *prometheus.CounterVec // chain, endpoint, status
	RPCDuration       *prometheus.HistogramVec
	RPCRetries        *prometheus.CounterVec
	LastProcessedTime *prometheus.GaugeVec

	SinkDeliveries *prometheus.CounterVec
	SinkTransfers  *prometheus.CounterVec
	SinkDuration   *prometheus.HistogramVec
}

// Chain is the per-chain view of Metrics used by one listener.
type Chain struct {
	Name              string
	HeadBlock         prometheus.Gauge
	SafeHeadBlock     prometheus.Gauge
	CursorBlock       prometheus.Gauge
	LagBlocks         prometheus.Gauge
	BlocksProcessed   prometheus.Counter
	TransfersDecoded  prometheus.Counter
	TransfersMatched  *prometheus.CounterVec // reason
	DecodeErrors      prometheus.Counter
	BlockErrors       prometheus.Counter
	DispatchRetries   prometheus.Counter
	LastProcessedTime prometheus.Gauge
	m                 *Metrics
}

// For returns the collectors labelled with chain (e.g. "tron", "ton").
func (m *Metrics) For(chain string) *Chain {
	l := prometheus.Labels{"chain": chain}
	return &Chain{
		Name:              chain,
		HeadBlock:         m.HeadBlock.With(l),
		SafeHeadBlock:     m.SafeHeadBlock.With(l),
		CursorBlock:       m.CursorBlock.With(l),
		LagBlocks:         m.LagBlocks.With(l),
		BlocksProcessed:   m.BlocksProcessed.With(l),
		TransfersDecoded:  m.TransfersDecoded.With(l),
		TransfersMatched:  m.TransfersMatched.MustCurryWith(l),
		DecodeErrors:      m.DecodeErrors.With(l),
		BlockErrors:       m.BlockErrors.With(l),
		DispatchRetries:   m.DispatchRetries.With(l),
		LastProcessedTime: m.LastProcessedTime.With(l),
		m:                 m,
	}
}

// ObserveRequest implements the chain clients' Observer interface.
func (c *Chain) ObserveRequest(endpoint, status string, d time.Duration) {
	c.m.RPCRequests.WithLabelValues(c.Name, endpoint, status).Inc()
	c.m.RPCDuration.WithLabelValues(c.Name, endpoint).Observe(d.Seconds())
}

// ObserveRetry implements the chain clients' Observer interface.
func (c *Chain) ObserveRetry(endpoint, reason string) {
	c.m.RPCRetries.WithLabelValues(c.Name, endpoint, reason).Inc()
}

// New creates and registers all metrics on a fresh registry.
func New() *Metrics {
	r := prometheus.NewRegistry()
	r.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	f := func(c prometheus.Collector) { r.MustRegister(c) }
	m := &Metrics{Registry: r}

	ch := []string{"chain"}
	m.HeadBlock = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: ns, Name: "head_block", Help: "Latest block number reported by the node (TON: masterchain seqno)."}, ch)
	m.SafeHeadBlock = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: ns, Name: "safe_head_block", Help: "Head minus configured confirmations."}, ch)
	m.CursorBlock = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: ns, Name: "cursor_block", Help: "Last fully processed block."}, ch)
	m.LagBlocks = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: ns, Name: "lag_blocks", Help: "Safe head minus cursor."}, ch)
	m.LastProcessedTime = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: ns, Name: "last_processed_timestamp_seconds", Help: "Unix time when the last block was processed."}, ch)
	m.BlocksProcessed = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "blocks_processed_total", Help: "Blocks fully processed."}, ch)
	m.TransfersDecoded = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "transfers_decoded_total", Help: "Token transfers decoded (before filtering)."}, ch)
	m.TransfersMatched = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "transfers_matched_total", Help: "Transfers matched by the filter, by chain and reason."}, []string{"chain", "reason"})
	m.DecodeErrors = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "decode_errors_total", Help: "Malformed token logs / transfer items skipped."}, ch)
	m.BlockErrors = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "block_fetch_errors_total", Help: "Block fetches that failed after retries."}, ch)
	m.DispatchRetries = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "dispatch_retries_total", Help: "Block deliveries retried because a required sink failed."}, ch)
	m.RPCRequests = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "rpc_requests_total", Help: "Chain API requests by chain, endpoint and status."}, []string{"chain", "endpoint", "status"})
	m.RPCDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: ns, Name: "rpc_request_duration_seconds", Help: "Chain API request latency.", Buckets: prometheus.DefBuckets}, []string{"chain", "endpoint"})
	m.RPCRetries = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "rpc_retries_total", Help: "Chain API retries by chain, endpoint and reason."}, []string{"chain", "endpoint", "reason"})
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

// ObserveSink implements sink.Recorder.
func (m *Metrics) ObserveSink(sink, status string, n int, d time.Duration) {
	m.SinkDeliveries.WithLabelValues(sink, status).Inc()
	m.SinkTransfers.WithLabelValues(sink, status).Add(float64(n))
	m.SinkDuration.WithLabelValues(sink).Observe(d.Seconds())
}
