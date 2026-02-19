// -------------------------------------------------------------------------------
// Replicator - Background Replica Creation Worker
//
// Author: Alex Freidah
//
// Creates additional copies of under-replicated objects across backends. Objects
// are written to one backend on PUT; this worker asynchronously ensures each
// object reaches the configured replication factor. Uses conditional DB inserts
// to safely handle concurrent overwrites and deletes.
// -------------------------------------------------------------------------------

package storage

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/afreidah/s3-orchestrator/internal/config"
	"github.com/afreidah/s3-orchestrator/internal/telemetry"
)

// -------------------------------------------------------------------------
// PUBLIC API
// -------------------------------------------------------------------------

// Replicate finds under-replicated objects and creates additional copies to
// reach the target replication factor. Returns the number of copies created.
func (m *BackendManager) Replicate(ctx context.Context, cfg config.ReplicationConfig) (int, error) {
	start := time.Now()

	// --- Find under-replicated objects ---
	locations, err := m.store.GetUnderReplicatedObjects(ctx, cfg.Factor, cfg.BatchSize)
	if err != nil {
		telemetry.ReplicationRunsTotal.WithLabelValues("error").Inc()
		return 0, fmt.Errorf("failed to query under-replicated objects: %w", err)
	}

	if len(locations) == 0 {
		telemetry.ReplicationRunsTotal.WithLabelValues("success").Inc()
		telemetry.ReplicationDuration.Observe(time.Since(start).Seconds())
		return 0, nil
	}

	// --- Group locations by object key ---
	grouped := groupByKey(locations)

	// --- Replicate each under-replicated object ---
	created := 0
	for key, copies := range grouped {
		needed := cfg.Factor - len(copies)
		if needed <= 0 {
			continue
		}

		n, err := m.replicateObject(ctx, key, copies, needed)
		if err != nil {
			slog.Warn("Replication: failed to replicate object",
				"key", key, "error", err)
			telemetry.ReplicationErrorsTotal.Inc()
			continue
		}
		created += n
	}

	telemetry.ReplicationCopiesCreatedTotal.Add(float64(created))
	telemetry.ReplicationPending.Set(float64(len(grouped)))
	telemetry.ReplicationRunsTotal.WithLabelValues("success").Inc()
	telemetry.ReplicationDuration.Observe(time.Since(start).Seconds())

	return created, nil
}

// -------------------------------------------------------------------------
// INTERNALS
// -------------------------------------------------------------------------

// groupByKey groups a flat list of object locations into a map keyed by object_key.
func groupByKey(locations []ObjectLocation) map[string][]ObjectLocation {
	grouped := make(map[string][]ObjectLocation)
	for _, loc := range locations {
		grouped[loc.ObjectKey] = append(grouped[loc.ObjectKey], loc)
	}
	return grouped
}

// replicateObject creates up to `needed` additional copies of a single object.
// Returns the number of copies successfully created.
func (m *BackendManager) replicateObject(ctx context.Context, key string, existingCopies []ObjectLocation, needed int) (int, error) {
	// Build exclusion set of backends that already hold a copy
	exclusion := make(map[string]bool, len(existingCopies))
	for _, c := range existingCopies {
		exclusion[c.BackendName] = true
	}

	created := 0
	for i := 0; i < needed; i++ {
		// --- Find a target backend with space ---
		target := m.findReplicaTarget(ctx, key, existingCopies[0].SizeBytes, exclusion)
		if target == "" {
			slog.Warn("Replication: no target backend with space",
				"key", key, "needed", needed-i)
			break
		}

		// --- Copy data from an existing copy to the target ---
		source, err := m.copyToReplica(ctx, key, existingCopies, target)
		if err != nil {
			slog.Warn("Replication: failed to copy object data",
				"key", key, "target", target, "error", err)
			telemetry.ReplicationErrorsTotal.Inc()
			continue
		}

		// --- Record the replica in the database (conditional insert) ---
		inserted, err := m.store.RecordReplica(ctx, key, target, source, existingCopies[0].SizeBytes)
		if err != nil {
			slog.Error("Replication: failed to record replica",
				"key", key, "target", target, "error", err)
			// Clean up orphan on target
			m.cleanupOrphan(ctx, target, key)
			telemetry.ReplicationErrorsTotal.Inc()
			continue
		}

		if !inserted {
			// Source copy was deleted/overwritten during replication
			slog.Info("Replication: source copy gone, cleaning up orphan",
				"key", key, "target", target)
			m.cleanupOrphan(ctx, target, key)
			continue
		}

		m.recordUsage(source, 1, existingCopies[0].SizeBytes, 0) // source: Get + egress
		m.recordUsage(target, 1, 0, existingCopies[0].SizeBytes) // target: Put + ingress

		exclusion[target] = true
		created++
	}

	return created, nil
}

// findReplicaTarget selects a backend that has enough space and doesn't already
// hold a copy. Returns empty string if no suitable target exists.
func (m *BackendManager) findReplicaTarget(ctx context.Context, key string, size int64, exclusion map[string]bool) string {
	stats, err := m.store.GetQuotaStats(ctx)
	if err != nil {
		slog.Warn("Replication: failed to get quota stats", "error", err)
		return ""
	}

	for _, name := range m.order {
		if exclusion[name] {
			continue
		}
		stat, ok := stats[name]
		if !ok {
			continue
		}
		if stat.BytesLimit-stat.BytesUsed >= size {
			return name
		}
	}

	return ""
}

// copyToReplica reads the object from an existing copy and writes it to the
// target backend. Tries each existing copy in order for failover. Returns the
// source backend name that was successfully read from.
func (m *BackendManager) copyToReplica(ctx context.Context, key string, copies []ObjectLocation, target string) (string, error) {
	targetBackend, ok := m.backends[target]
	if !ok {
		return "", fmt.Errorf("target backend %s not found", target)
	}

	// Try each copy in order (primary first)
	for _, copy := range copies {
		srcBackend, ok := m.backends[copy.BackendName]
		if !ok {
			continue
		}

		rctx, rcancel := m.withTimeout(ctx)
		result, err := srcBackend.GetObject(rctx, key, "")
		rcancel()
		if err != nil {
			slog.Warn("Replication: source read failed, trying next copy",
				"key", key, "source", copy.BackendName, "error", err)
			continue
		}

		wctx, wcancel := m.withTimeout(ctx)
		_, err = targetBackend.PutObject(wctx, key, result.Body, result.Size, result.ContentType)
		_ = result.Body.Close()
		wcancel()
		if err != nil {
			return "", fmt.Errorf("failed to write to target %s: %w", target, err)
		}

		return copy.BackendName, nil
	}

	return "", fmt.Errorf("all source copies failed for key %s", key)
}

// cleanupOrphan deletes an object from a backend when the DB record was not
// created (e.g. source was deleted during replication).
func (m *BackendManager) cleanupOrphan(ctx context.Context, backendName, key string) {
	backend, ok := m.backends[backendName]
	if !ok {
		return
	}
	dctx, dcancel := m.withTimeout(ctx)
	defer dcancel()
	if err := backend.DeleteObject(dctx, key); err != nil {
		slog.Warn("Replication: failed to clean up orphan",
			"key", key, "backend", backendName, "error", err)
	}
}
