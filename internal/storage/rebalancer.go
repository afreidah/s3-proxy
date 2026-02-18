// -------------------------------------------------------------------------------
// Rebalancer - Periodic Backend Object Distribution
//
// Project: Munchbox / Author: Alex Freidah
//
// Moves objects between backends to optimize space distribution. Supports two
// strategies: "pack" consolidates free space by filling backends in order, and
// "spread" equalizes utilization ratios across all backends. Disabled by default
// to avoid unexpected API calls and egress charges.
// -------------------------------------------------------------------------------

package storage

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/afreidah/s3-proxy/internal/config"
	"github.com/afreidah/s3-proxy/internal/telemetry"
)

// -------------------------------------------------------------------------
// TYPES
// -------------------------------------------------------------------------

// rebalanceMove describes a single object move from one backend to another.
type rebalanceMove struct {
	ObjectKey   string
	FromBackend string
	ToBackend   string
	SizeBytes   int64
}

// -------------------------------------------------------------------------
// PUBLIC API
// -------------------------------------------------------------------------

// Rebalance moves objects between backends to optimize space distribution.
// Returns the number of objects successfully moved.
func (m *BackendManager) Rebalance(ctx context.Context, cfg config.RebalanceConfig) (int, error) {
	start := time.Now()

	// --- Get current quota stats ---
	stats, err := m.store.GetQuotaStats(ctx)
	if err != nil {
		telemetry.RebalanceRunsTotal.WithLabelValues(cfg.Strategy, "error").Inc()
		return 0, fmt.Errorf("failed to get quota stats: %w", err)
	}

	// --- Check threshold ---
	if !exceedsThreshold(stats, m.order, cfg.Threshold) {
		slog.Info("Rebalance skipping, within threshold",
			"threshold", cfg.Threshold, "strategy", cfg.Strategy)
		telemetry.RebalanceSkipped.WithLabelValues("threshold").Inc()
		return 0, nil
	}

	// --- Compute plan ---
	var plan []rebalanceMove
	switch cfg.Strategy {
	case "pack":
		plan, err = m.planPackTight(ctx, stats, cfg.BatchSize)
	case "spread":
		plan, err = m.planSpreadEven(ctx, stats, cfg.BatchSize)
	default:
		return 0, fmt.Errorf("unknown rebalance strategy: %s", cfg.Strategy)
	}
	if err != nil {
		telemetry.RebalanceRunsTotal.WithLabelValues(cfg.Strategy, "error").Inc()
		return 0, fmt.Errorf("failed to plan rebalance: %w", err)
	}

	if len(plan) == 0 {
		slog.Info("Rebalance skipping, empty plan", "strategy", cfg.Strategy)
		telemetry.RebalanceSkipped.WithLabelValues("empty_plan").Inc()
		return 0, nil
	}

	// --- Execute moves ---
	moved := m.executeMoves(ctx, plan, cfg.Strategy)

	telemetry.RebalanceRunsTotal.WithLabelValues(cfg.Strategy, "success").Inc()
	telemetry.RebalanceDuration.WithLabelValues(cfg.Strategy).Observe(time.Since(start).Seconds())

	return moved, nil
}

// -------------------------------------------------------------------------
// THRESHOLD CHECK
// -------------------------------------------------------------------------

// exceedsThreshold returns true if the utilization spread across backends
// exceeds the configured threshold.
func exceedsThreshold(stats map[string]QuotaStat, order []string, threshold float64) bool {
	if len(order) < 2 {
		return false
	}

	var minRatio, maxRatio float64
	first := true

	for _, name := range order {
		stat, ok := stats[name]
		if !ok || stat.BytesLimit == 0 {
			continue
		}
		ratio := float64(stat.BytesUsed) / float64(stat.BytesLimit)
		if first {
			minRatio = ratio
			maxRatio = ratio
			first = false
		} else {
			if ratio < minRatio {
				minRatio = ratio
			}
			if ratio > maxRatio {
				maxRatio = ratio
			}
		}
	}

	return maxRatio-minRatio >= threshold
}

// -------------------------------------------------------------------------
// PACK TIGHT STRATEGY
// -------------------------------------------------------------------------

// planPackTight consolidates objects onto the most-utilized backends, pulling
// from the least-utilized. Sorts by percent full descending and only moves
// objects from a less-full source to a more-full destination. Skips moves
// that would not increase the destination's packing ratio.
func (m *BackendManager) planPackTight(ctx context.Context, stats map[string]QuotaStat, batchSize int) ([]rebalanceMove, error) {
	type backendUtil struct {
		Name  string
		Limit int64
	}

	// --- Build sorted backend list (most full first) ---
	simUsed := make(map[string]int64)
	var backends []backendUtil

	for _, name := range m.order {
		stat, ok := stats[name]
		if !ok || stat.BytesLimit == 0 {
			continue
		}
		simUsed[name] = stat.BytesUsed
		backends = append(backends, backendUtil{Name: name, Limit: stat.BytesLimit})
	}

	sort.Slice(backends, func(i, j int) bool {
		ri := float64(simUsed[backends[i].Name]) / float64(backends[i].Limit)
		rj := float64(simUsed[backends[j].Name]) / float64(backends[j].Limit)
		return ri > rj
	})

	var plan []rebalanceMove
	remaining := batchSize

	// --- Pack into most-full destinations, pulling from least-full sources ---
	for di := 0; di < len(backends) && remaining > 0; di++ {
		dest := backends[di]
		destFree := dest.Limit - simUsed[dest.Name]
		if destFree <= 0 {
			continue
		}

		for si := len(backends) - 1; si > di && remaining > 0 && destFree > 0; si-- {
			src := backends[si]

			// Only pull from backends that are less utilized
			srcRatio := float64(simUsed[src.Name]) / float64(src.Limit)
			destRatio := float64(simUsed[dest.Name]) / float64(dest.Limit)
			if srcRatio >= destRatio {
				continue
			}

			objects, err := m.store.ListObjectsByBackend(ctx, src.Name, remaining)
			if err != nil {
				return nil, fmt.Errorf("failed to list objects on %s: %w", src.Name, err)
			}

			for _, obj := range objects {
				if remaining <= 0 || destFree <= 0 {
					break
				}
				if obj.SizeBytes > destFree {
					continue
				}

				// Re-check ratios after prior simulated moves
				srcRatio = float64(simUsed[src.Name]) / float64(src.Limit)
				destRatio = float64(simUsed[dest.Name]) / float64(dest.Limit)
				if srcRatio >= destRatio {
					break // Source is now as full or fuller, stop pulling
				}

				plan = append(plan, rebalanceMove{
					ObjectKey:   obj.ObjectKey,
					FromBackend: src.Name,
					ToBackend:   dest.Name,
					SizeBytes:   obj.SizeBytes,
				})

				destFree -= obj.SizeBytes
				simUsed[dest.Name] += obj.SizeBytes
				simUsed[src.Name] -= obj.SizeBytes
				remaining--
			}
		}
	}

	return plan, nil
}

// -------------------------------------------------------------------------
// SPREAD EVEN STRATEGY
// -------------------------------------------------------------------------

// backendBalance tracks a backend's excess or deficit relative to the target.
type backendBalance struct {
	Name    string
	Balance int64 // positive = over-target (source), negative = under-target (dest)
}

// planSpreadEven equalizes utilization ratios across backends. Moves objects
// from over-utilized backends to under-utilized ones.
func (m *BackendManager) planSpreadEven(ctx context.Context, stats map[string]QuotaStat, batchSize int) ([]rebalanceMove, error) {
	var totalUsed, totalLimit int64
	for _, name := range m.order {
		stat, ok := stats[name]
		if !ok {
			continue
		}
		totalUsed += stat.BytesUsed
		totalLimit += stat.BytesLimit
	}

	if totalLimit == 0 {
		return nil, nil
	}

	targetRatio := float64(totalUsed) / float64(totalLimit)

	// Compute excess/deficit for each backend
	var sources, destinations []backendBalance
	simUsed := make(map[string]int64)

	for _, name := range m.order {
		stat, ok := stats[name]
		if !ok {
			continue
		}
		simUsed[name] = stat.BytesUsed
		targetBytes := int64(targetRatio * float64(stat.BytesLimit))
		excess := stat.BytesUsed - targetBytes

		if excess > 0 {
			sources = append(sources, backendBalance{Name: name, Balance: excess})
		} else if excess < 0 {
			destinations = append(destinations, backendBalance{Name: name, Balance: excess})
		}
	}

	// Sort: most over-target sources first, most under-target destinations first
	sort.Slice(sources, func(i, j int) bool {
		return sources[i].Balance > sources[j].Balance
	})
	sort.Slice(destinations, func(i, j int) bool {
		return destinations[i].Balance < destinations[j].Balance
	})

	var plan []rebalanceMove
	remaining := batchSize

	for si := range sources {
		if remaining <= 0 {
			break
		}

		src := &sources[si]
		objects, err := m.store.ListObjectsByBackend(ctx, src.Name, remaining)
		if err != nil {
			return nil, fmt.Errorf("failed to list objects on %s: %w", src.Name, err)
		}

		for _, obj := range objects {
			if remaining <= 0 || src.Balance <= 0 {
				break
			}

			// Skip if this object is larger than what the source needs to shed —
			// moving it would overshoot and make the source under-target
			if obj.SizeBytes > src.Balance {
				continue
			}

			// Find best destination that can fit this object without overshooting
			bestDest := -1
			for di := range destinations {
				deficit := -destinations[di].Balance
				destStat := stats[destinations[di].Name]
				destFree := destStat.BytesLimit - simUsed[destinations[di].Name]

				if deficit >= obj.SizeBytes && obj.SizeBytes <= destFree {
					bestDest = di
					break
				}
			}

			if bestDest < 0 {
				continue
			}

			dest := &destinations[bestDest]
			plan = append(plan, rebalanceMove{
				ObjectKey:   obj.ObjectKey,
				FromBackend: src.Name,
				ToBackend:   dest.Name,
				SizeBytes:   obj.SizeBytes,
			})

			src.Balance -= obj.SizeBytes
			dest.Balance += obj.SizeBytes
			simUsed[src.Name] -= obj.SizeBytes
			simUsed[dest.Name] += obj.SizeBytes
			remaining--
		}
	}

	return plan, nil
}

// -------------------------------------------------------------------------
// MOVE EXECUTION
// -------------------------------------------------------------------------

// executeMoves performs the planned object moves sequentially. Skips individual
// moves that fail and continues with the rest.
func (m *BackendManager) executeMoves(ctx context.Context, plan []rebalanceMove, strategy string) int {
	moved := 0

	for _, move := range plan {
		srcBackend, ok := m.backends[move.FromBackend]
		if !ok {
			slog.Error("Rebalance: source backend not found", "backend", move.FromBackend)
			continue
		}

		destBackend, ok := m.backends[move.ToBackend]
		if !ok {
			slog.Error("Rebalance: destination backend not found", "backend", move.ToBackend)
			continue
		}

		// --- Read from source ---
		result, err := srcBackend.GetObject(ctx, move.ObjectKey, "")
		if err != nil {
			slog.Warn("Rebalance: failed to read source object",
				"key", move.ObjectKey, "backend", move.FromBackend, "error", err)
			telemetry.RebalanceObjectsMoved.WithLabelValues(strategy, "error").Inc()
			continue
		}

		// --- Write to destination ---
		_, err = destBackend.PutObject(ctx, move.ObjectKey, result.Body, result.Size, result.ContentType)
		_ = result.Body.Close()
		if err != nil {
			slog.Warn("Rebalance: failed to write destination object",
				"key", move.ObjectKey, "backend", move.ToBackend, "error", err)
			telemetry.RebalanceObjectsMoved.WithLabelValues(strategy, "error").Inc()
			continue
		}

		// --- Atomic DB update (compare-and-swap) ---
		movedSize, err := m.store.MoveObjectLocation(ctx, move.ObjectKey, move.FromBackend, move.ToBackend)
		if err != nil {
			slog.Error("Rebalance: failed to update object location",
				"key", move.ObjectKey, "error", err)
			// Clean up orphan on destination
			if delErr := destBackend.DeleteObject(ctx, move.ObjectKey); delErr != nil {
				slog.Warn("Rebalance: failed to clean up orphan", "key", move.ObjectKey, "error", delErr)
			}
			telemetry.RebalanceObjectsMoved.WithLabelValues(strategy, "error").Inc()
			continue
		}

		if movedSize == 0 {
			// Object was deleted or already moved by another process
			slog.Info("Rebalance: object already moved or deleted, cleaning up",
				"key", move.ObjectKey)
			if delErr := destBackend.DeleteObject(ctx, move.ObjectKey); delErr != nil {
				slog.Warn("Rebalance: failed to clean up orphan", "key", move.ObjectKey, "error", delErr)
			}
			continue
		}

		// --- Delete from source ---
		if err := srcBackend.DeleteObject(ctx, move.ObjectKey); err != nil {
			slog.Warn("Rebalance: failed to delete source object (orphan)",
				"key", move.ObjectKey, "backend", move.FromBackend, "error", err)
			// DB is correct, source just has a leftover copy
		}

		moved++
		telemetry.RebalanceObjectsMoved.WithLabelValues(strategy, "success").Inc()
		telemetry.RebalanceBytesMoved.WithLabelValues(strategy).Add(float64(movedSize))
	}

	return moved
}
