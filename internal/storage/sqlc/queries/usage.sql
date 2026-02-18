-- name: FlushUsageDeltas :exec
-- Atomically adds accumulated in-memory deltas to the persistent usage row.
-- Creates the row if it doesn't exist for this (backend, period) yet.
INSERT INTO backend_usage (backend_name, period, api_requests, egress_bytes, ingress_bytes, updated_at)
VALUES (@backend_name, @period, @api_requests, @egress_bytes, @ingress_bytes, NOW())
ON CONFLICT (backend_name, period) DO UPDATE SET
    api_requests  = backend_usage.api_requests  + @api_requests,
    egress_bytes  = backend_usage.egress_bytes  + @egress_bytes,
    ingress_bytes = backend_usage.ingress_bytes + @ingress_bytes,
    updated_at    = NOW();

-- name: GetUsageForPeriod :many
-- Returns usage stats for all backends in the given period (e.g. '2026-02').
SELECT backend_name, api_requests, egress_bytes, ingress_bytes
FROM backend_usage
WHERE period = @period;
