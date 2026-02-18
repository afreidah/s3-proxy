-- Schema for sqlc code generation (mirrors migration.sql)

CREATE TABLE backend_quotas (
    backend_name TEXT PRIMARY KEY,
    bytes_used   BIGINT NOT NULL DEFAULT 0,
    bytes_limit  BIGINT NOT NULL,
    updated_at   TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE object_locations (
    object_key   TEXT NOT NULL,
    backend_name TEXT NOT NULL REFERENCES backend_quotas(backend_name),
    size_bytes   BIGINT NOT NULL,
    created_at   TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (object_key, backend_name)
);

CREATE INDEX idx_object_locations_backend
    ON object_locations(backend_name);

CREATE INDEX idx_object_locations_key_pattern
    ON object_locations(object_key text_pattern_ops);

CREATE INDEX idx_object_locations_created
    ON object_locations(created_at);

CREATE TABLE multipart_uploads (
    upload_id    TEXT PRIMARY KEY,
    object_key   TEXT NOT NULL,
    backend_name TEXT NOT NULL REFERENCES backend_quotas(backend_name),
    content_type TEXT,
    created_at   TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE multipart_parts (
    upload_id   TEXT NOT NULL REFERENCES multipart_uploads(upload_id) ON DELETE CASCADE,
    part_number INT NOT NULL,
    etag        TEXT NOT NULL,
    size_bytes  BIGINT NOT NULL,
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (upload_id, part_number)
);

CREATE INDEX idx_multipart_uploads_created
    ON multipart_uploads(created_at);

CREATE TABLE backend_usage (
    backend_name  TEXT NOT NULL REFERENCES backend_quotas(backend_name),
    period        TEXT NOT NULL,
    api_requests  BIGINT NOT NULL DEFAULT 0,
    egress_bytes  BIGINT NOT NULL DEFAULT 0,
    ingress_bytes BIGINT NOT NULL DEFAULT 0,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (backend_name, period)
);
