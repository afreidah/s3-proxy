// -------------------------------------------------------------------------------
// Metrics - Prometheus Instrumentation
//
// Author: Alex Freidah
//
// Prometheus metric definitions for the S3 proxy. Tracks request counts, latencies,
// sizes, and backend health. All metrics are prefixed with 's3proxy_' for easy
// identification in dashboards and alerting rules.
// -------------------------------------------------------------------------------

package telemetry

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// -------------------------------------------------------------------------
// METRIC DEFINITIONS
// -------------------------------------------------------------------------

var (
	// --- Request metrics ---

	// RequestsTotal counts all HTTP requests by method and status code.
	RequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "s3proxy_requests_total",
			Help: "Total number of HTTP requests processed",
		},
		[]string{"method", "status_code"},
	)

	// RequestDuration tracks request latency distribution by method.
	RequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "s3proxy_request_duration_seconds",
			Help:    "HTTP request latency in seconds",
			Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60},
		},
		[]string{"method"},
	)

	// RequestSize tracks upload sizes.
	RequestSize = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "s3proxy_request_size_bytes",
			Help:    "HTTP request body size in bytes",
			Buckets: prometheus.ExponentialBuckets(1024, 4, 10), // 1KB to 256GB
		},
		[]string{"method"},
	)

	// ResponseSize tracks download sizes.
	ResponseSize = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "s3proxy_response_size_bytes",
			Help:    "HTTP response body size in bytes",
			Buckets: prometheus.ExponentialBuckets(1024, 4, 10), // 1KB to 256GB
		},
		[]string{"method"},
	)

	// InflightRequests tracks currently processing requests.
	InflightRequests = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "s3proxy_inflight_requests",
			Help: "Number of requests currently being processed",
		},
		[]string{"method"},
	)

	// --- Backend metrics ---

	// BackendRequestsTotal counts backend operations by operation type and status.
	BackendRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "s3proxy_backend_requests_total",
			Help: "Total number of backend storage operations",
		},
		[]string{"operation", "backend", "status"},
	)

	// BackendDuration tracks backend operation latency.
	BackendDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "s3proxy_backend_duration_seconds",
			Help:    "Backend operation latency in seconds",
			Buckets: []float64{.01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120},
		},
		[]string{"operation", "backend"},
	)

	// --- Manager metrics ---

	// ManagerRequestsTotal counts manager-level operations.
	ManagerRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "s3proxy_manager_requests_total",
			Help: "Total number of manager-level storage operations",
		},
		[]string{"operation", "backend", "status"},
	)

	// ManagerDuration tracks manager operation latency.
	ManagerDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "s3proxy_manager_duration_seconds",
			Help:    "Manager operation latency in seconds",
			Buckets: []float64{.01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120},
		},
		[]string{"operation", "backend"},
	)

	// --- Quota metrics ---

	// QuotaBytesUsed tracks current bytes used per backend.
	QuotaBytesUsed = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "s3proxy_quota_bytes_used",
			Help: "Current bytes used on each backend",
		},
		[]string{"backend"},
	)

	// QuotaBytesLimit tracks quota limit per backend.
	QuotaBytesLimit = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "s3proxy_quota_bytes_limit",
			Help: "Quota limit in bytes for each backend",
		},
		[]string{"backend"},
	)

	// QuotaBytesAvailable tracks available bytes per backend.
	QuotaBytesAvailable = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "s3proxy_quota_bytes_available",
			Help: "Available bytes (limit - used) for each backend",
		},
		[]string{"backend"},
	)

	// --- Object metrics ---

	// ObjectCount tracks the number of objects stored per backend.
	ObjectCount = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "s3proxy_objects_count",
			Help: "Number of objects stored on each backend",
		},
		[]string{"backend"},
	)

	// --- Multipart metrics ---

	// ActiveMultipartUploads tracks in-progress multipart uploads per backend.
	ActiveMultipartUploads = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "s3proxy_active_multipart_uploads",
			Help: "Number of in-progress multipart uploads per backend",
		},
		[]string{"backend"},
	)

	// --- Usage tracking metrics ---

	// UsageApiRequests tracks the current month's API request count per backend.
	UsageApiRequests = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "s3proxy_usage_api_requests",
			Help: "Current month API request count per backend (from DB)",
		},
		[]string{"backend"},
	)

	// UsageEgressBytes tracks the current month's egress bytes per backend.
	UsageEgressBytes = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "s3proxy_usage_egress_bytes",
			Help: "Current month egress bytes per backend (from DB)",
		},
		[]string{"backend"},
	)

	// UsageIngressBytes tracks the current month's ingress bytes per backend.
	UsageIngressBytes = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "s3proxy_usage_ingress_bytes",
			Help: "Current month ingress bytes per backend (from DB)",
		},
		[]string{"backend"},
	)

	// --- Rebalancer metrics ---

	// RebalanceObjectsMoved counts objects moved by the rebalancer.
	RebalanceObjectsMoved = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "s3proxy_rebalance_objects_moved_total",
			Help: "Total number of objects moved by the rebalancer",
		},
		[]string{"strategy", "status"},
	)

	// RebalanceBytesMoved counts bytes moved by the rebalancer.
	RebalanceBytesMoved = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "s3proxy_rebalance_bytes_moved_total",
			Help: "Total bytes moved by the rebalancer",
		},
		[]string{"strategy"},
	)

	// RebalanceRunsTotal counts rebalancer executions.
	RebalanceRunsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "s3proxy_rebalance_runs_total",
			Help: "Total number of rebalancer runs",
		},
		[]string{"strategy", "status"},
	)

	// RebalanceDuration tracks rebalancer execution time.
	RebalanceDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "s3proxy_rebalance_duration_seconds",
			Help:    "Rebalancer execution time in seconds",
			Buckets: []float64{1, 5, 10, 30, 60, 120, 300, 600},
		},
		[]string{"strategy"},
	)

	// RebalanceSkipped counts rebalancer runs that were skipped.
	RebalanceSkipped = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "s3proxy_rebalance_skipped_total",
			Help: "Total number of rebalancer runs skipped",
		},
		[]string{"reason"},
	)

	// --- Replication metrics ---

	// ReplicationPending tracks objects currently below the target replication factor.
	ReplicationPending = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "s3proxy_replication_pending",
			Help: "Number of objects below the target replication factor",
		},
	)

	// ReplicationCopiesCreatedTotal counts replica copies created.
	ReplicationCopiesCreatedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "s3proxy_replication_copies_created_total",
			Help: "Total number of replica copies created",
		},
	)

	// ReplicationErrorsTotal counts replication errors.
	ReplicationErrorsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "s3proxy_replication_errors_total",
			Help: "Total number of replication errors",
		},
	)

	// ReplicationDuration tracks replication worker cycle time.
	ReplicationDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "s3proxy_replication_duration_seconds",
			Help:    "Replication worker cycle time in seconds",
			Buckets: []float64{1, 5, 10, 30, 60, 120, 300, 600},
		},
	)

	// ReplicationRunsTotal counts replication worker executions.
	ReplicationRunsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "s3proxy_replication_runs_total",
			Help: "Total number of replication worker runs",
		},
		[]string{"status"},
	)

	// --- Circuit breaker metrics ---

	// CircuitBreakerState tracks the current circuit breaker state.
	// 0=closed (healthy), 1=open (DB down), 2=half-open (probing).
	CircuitBreakerState = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "s3proxy_circuit_breaker_state",
			Help: "Current circuit breaker state: 0=closed, 1=open, 2=half-open",
		},
	)

	// CircuitBreakerTransitionsTotal counts state transitions.
	CircuitBreakerTransitionsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "s3proxy_circuit_breaker_transitions_total",
			Help: "Total number of circuit breaker state transitions",
		},
		[]string{"from", "to"},
	)

	// DegradedReadsTotal counts reads served via broadcast during degraded mode.
	DegradedReadsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "s3proxy_degraded_reads_total",
			Help: "Total number of read operations served via broadcast during degraded mode",
		},
		[]string{"operation"},
	)

	// DegradedCacheHitsTotal counts location cache hits during degraded reads.
	DegradedCacheHitsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "s3proxy_degraded_cache_hits_total",
			Help: "Total number of location cache hits during degraded reads",
		},
	)

	// DegradedWriteRejectionsTotal counts writes rejected during degraded mode.
	DegradedWriteRejectionsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "s3proxy_degraded_write_rejections_total",
			Help: "Total number of write operations rejected during degraded mode",
		},
		[]string{"operation"},
	)

	// --- Info metric ---

	// BuildInfo exposes version information.
	BuildInfo = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "s3proxy_build_info",
			Help: "Build information for the S3 proxy",
		},
		[]string{"version", "go_version"},
	)
)
