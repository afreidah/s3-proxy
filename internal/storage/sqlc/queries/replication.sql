-- name: GetUnderReplicatedObjects :many
WITH under_replicated AS (
    SELECT object_key
    FROM object_locations
    GROUP BY object_key
    HAVING COUNT(*) < @factor::bigint
    LIMIT @max_keys
)
SELECT ol.object_key, ol.backend_name, ol.size_bytes, ol.created_at
FROM object_locations ol
JOIN under_replicated ur ON ol.object_key = ur.object_key
ORDER BY ol.object_key, ol.created_at ASC;

-- name: InsertReplicaConditional :one
INSERT INTO object_locations (object_key, backend_name, size_bytes, created_at)
SELECT $1, $2, ol.size_bytes, NOW()
FROM object_locations ol
WHERE ol.object_key = $1 AND ol.backend_name = $3
ON CONFLICT (object_key, backend_name) DO NOTHING
RETURNING true AS inserted;
