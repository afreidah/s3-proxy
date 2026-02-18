package storage

import (
	"context"
	"log/slog"
	"time"
)

// currentPeriod returns the current month as "YYYY-MM" for usage aggregation.
func currentPeriod() string {
	return time.Now().UTC().Format("2006-01")
}

// FlushUsage reads and resets the in-memory atomic counters, then writes the
// accumulated deltas to the database. Called periodically (every 30s). On DB
// error, deltas are added back to avoid data loss.
func (m *BackendManager) FlushUsage(ctx context.Context) error {
	period := currentPeriod()
	var lastErr error

	for name, counters := range m.usage {
		apiReqs := counters.apiRequests.Swap(0)
		egress := counters.egressBytes.Swap(0)
		ingress := counters.ingressBytes.Swap(0)

		if apiReqs == 0 && egress == 0 && ingress == 0 {
			continue
		}

		if err := m.store.FlushUsageDeltas(ctx, name, period, apiReqs, egress, ingress); err != nil {
			// Restore deltas so they aren't lost
			counters.apiRequests.Add(apiReqs)
			counters.egressBytes.Add(egress)
			counters.ingressBytes.Add(ingress)
			slog.Error("Failed to flush usage deltas", "backend", name, "error", err)
			lastErr = err
		}
	}

	return lastErr
}
