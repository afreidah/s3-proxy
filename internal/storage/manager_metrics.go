package storage

import (
	"context"
	"log/slog"

	"github.com/afreidah/s3-proxy/internal/telemetry"
)

// -------------------------------------------------------------------------
// QUOTA METRICS
// -------------------------------------------------------------------------

// UpdateQuotaMetrics fetches quota stats, object counts, and active multipart
// upload counts, then updates the corresponding Prometheus gauges.
func (m *BackendManager) UpdateQuotaMetrics(ctx context.Context) error {
	stats, err := m.store.GetQuotaStats(ctx)
	if err != nil {
		return err
	}

	for name, stat := range stats {
		telemetry.QuotaBytesUsed.WithLabelValues(name).Set(float64(stat.BytesUsed))
		telemetry.QuotaBytesLimit.WithLabelValues(name).Set(float64(stat.BytesLimit))
		telemetry.QuotaBytesAvailable.WithLabelValues(name).Set(float64(stat.BytesLimit - stat.BytesUsed))
	}

	// --- Object counts per backend ---
	objCounts, err := m.store.GetObjectCounts(ctx)
	if err != nil {
		slog.Error("Failed to get object counts", "error", err)
	} else {
		// Reset to zero for backends with no objects, then set actual counts
		for name := range stats {
			telemetry.ObjectCount.WithLabelValues(name).Set(0)
		}
		for name, count := range objCounts {
			telemetry.ObjectCount.WithLabelValues(name).Set(float64(count))
		}
	}

	// --- Active multipart uploads per backend ---
	mpCounts, err := m.store.GetActiveMultipartCounts(ctx)
	if err != nil {
		slog.Error("Failed to get multipart upload counts", "error", err)
	} else {
		for name := range stats {
			telemetry.ActiveMultipartUploads.WithLabelValues(name).Set(0)
		}
		for name, count := range mpCounts {
			telemetry.ActiveMultipartUploads.WithLabelValues(name).Set(float64(count))
		}
	}

	return nil
}
