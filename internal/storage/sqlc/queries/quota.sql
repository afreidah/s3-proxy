-- name: UpsertQuotaLimit :exec
INSERT INTO backend_quotas (backend_name, bytes_limit, bytes_used, updated_at)
VALUES ($1, $2, 0, NOW())
ON CONFLICT (backend_name) DO UPDATE SET
    bytes_limit = $2,
    updated_at = NOW();

-- name: GetBackendAvailableSpace :one
SELECT (q.bytes_limit - q.bytes_used - COALESCE(m.inflight, 0))::bigint AS available
FROM backend_quotas q
LEFT JOIN (
    SELECT mu.backend_name, SUM(mp.size_bytes) AS inflight
    FROM multipart_uploads mu
    JOIN multipart_parts mp ON mp.upload_id = mu.upload_id
    GROUP BY mu.backend_name
) m ON m.backend_name = q.backend_name
WHERE q.backend_name = $1;

-- name: GetAllQuotaStats :many
SELECT backend_name, bytes_used, bytes_limit, updated_at
FROM backend_quotas;

-- name: GetObjectCountsByBackend :many
SELECT backend_name, COUNT(*) AS object_count
FROM object_locations
GROUP BY backend_name;

-- name: GetActiveMultipartCountsByBackend :many
SELECT backend_name, COUNT(*) AS upload_count
FROM multipart_uploads
GROUP BY backend_name;

-- name: IncrementQuota :exec
UPDATE backend_quotas
SET bytes_used = bytes_used + @amount, updated_at = NOW()
WHERE backend_name = @backend_name;

-- name: DecrementQuota :exec
UPDATE backend_quotas
SET bytes_used = GREATEST(0, bytes_used - @amount), updated_at = NOW()
WHERE backend_name = @backend_name;
