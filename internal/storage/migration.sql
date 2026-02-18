-- -------------------------------------------------------------------------------
-- S3 Proxy Database Schema
--
-- Author: Alex Freidah
--
-- PostgreSQL schema for quota tracking, object location storage, and multipart
-- uploads. Applied automatically on startup via go:embed.
-- -------------------------------------------------------------------------------

-- Track quota usage per backend
CREATE TABLE IF NOT EXISTS backend_quotas (
    backend_name TEXT PRIMARY KEY,
    bytes_used   BIGINT NOT NULL DEFAULT 0,
    bytes_limit  BIGINT NOT NULL,
    updated_at   TIMESTAMPTZ DEFAULT NOW()
);

-- Track which backend stores which object (composite PK supports replication)
CREATE TABLE IF NOT EXISTS object_locations (
    object_key   TEXT NOT NULL,
    backend_name TEXT NOT NULL REFERENCES backend_quotas(backend_name),
    size_bytes   BIGINT NOT NULL,
    created_at   TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (object_key, backend_name)
);

-- Index for efficient backend lookups
CREATE INDEX IF NOT EXISTS idx_object_locations_backend
    ON object_locations(backend_name);

-- Index for efficient LIKE prefix queries on object keys
CREATE INDEX IF NOT EXISTS idx_object_locations_key_pattern
    ON object_locations(object_key text_pattern_ops);

-- Index for cleanup queries
CREATE INDEX IF NOT EXISTS idx_object_locations_created
    ON object_locations(created_at);

-- Track in-progress multipart uploads
CREATE TABLE IF NOT EXISTS multipart_uploads (
    upload_id    TEXT PRIMARY KEY,
    object_key   TEXT NOT NULL,
    backend_name TEXT NOT NULL REFERENCES backend_quotas(backend_name),
    content_type TEXT,
    created_at   TIMESTAMPTZ DEFAULT NOW()
);

-- Track individual parts of multipart uploads
CREATE TABLE IF NOT EXISTS multipart_parts (
    upload_id   TEXT NOT NULL REFERENCES multipart_uploads(upload_id) ON DELETE CASCADE,
    part_number INT NOT NULL,
    etag        TEXT NOT NULL,
    size_bytes  BIGINT NOT NULL,
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (upload_id, part_number)
);

-- Index for cleaning up stale multipart uploads
CREATE INDEX IF NOT EXISTS idx_multipart_uploads_created
    ON multipart_uploads(created_at);

-- Track per-backend API requests and data transfer by month
CREATE TABLE IF NOT EXISTS backend_usage (
    backend_name  TEXT NOT NULL REFERENCES backend_quotas(backend_name),
    period        TEXT NOT NULL,
    api_requests  BIGINT NOT NULL DEFAULT 0,
    egress_bytes  BIGINT NOT NULL DEFAULT 0,
    ingress_bytes BIGINT NOT NULL DEFAULT 0,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (backend_name, period)
);
