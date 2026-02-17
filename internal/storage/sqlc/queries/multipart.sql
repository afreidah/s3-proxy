-- name: CreateMultipartUpload :exec
INSERT INTO multipart_uploads (upload_id, object_key, backend_name, content_type, created_at)
VALUES ($1, $2, $3, $4, NOW());

-- name: GetMultipartUpload :one
SELECT upload_id, object_key, backend_name, content_type, created_at
FROM multipart_uploads
WHERE upload_id = $1;

-- name: UpsertPart :exec
INSERT INTO multipart_parts (upload_id, part_number, etag, size_bytes, created_at)
VALUES ($1, $2, $3, $4, NOW())
ON CONFLICT (upload_id, part_number) DO UPDATE SET
    etag = $3, size_bytes = $4, created_at = NOW();

-- name: GetParts :many
SELECT part_number, etag, size_bytes, created_at
FROM multipart_parts
WHERE upload_id = $1
ORDER BY part_number;

-- name: DeleteMultipartUpload :exec
DELETE FROM multipart_uploads
WHERE upload_id = $1;

-- name: GetStaleMultipartUploads :many
SELECT upload_id, object_key, backend_name, content_type, created_at
FROM multipart_uploads
WHERE created_at < $1;
