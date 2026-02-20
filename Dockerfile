# -------------------------------------------------------------------------------
# S3 Orchestrator - Unified S3 Endpoint
#
# Author: Alex Freidah
#
# Go-based S3 orchestrator with Prometheus metrics and OpenTelemetry tracing.
# Provides a unified endpoint for S3-compatible storage backends.
# -------------------------------------------------------------------------------

FROM golang:1.26-alpine AS builder

ARG VERSION=dev

WORKDIR /build

# Install build dependencies
RUN apk add --no-cache git ca-certificates

# Copy go module files and download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY cmd/ cmd/
COPY internal/ internal/

# Build binary
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-s -w -X github.com/afreidah/s3-orchestrator/internal/telemetry.Version=${VERSION}" \
    -o s3-orchestrator ./cmd/s3-orchestrator

# -------------------------------------------------------------------------
# Runtime Image
# -------------------------------------------------------------------------

FROM alpine:3.21

RUN apk add --no-cache ca-certificates && \
    adduser -D -u 10001 appuser

COPY --from=builder /build/s3-orchestrator /usr/local/bin/

USER appuser

EXPOSE 9000

ENTRYPOINT ["s3-orchestrator"]
CMD ["-config", "/etc/s3-orchestrator/config.yaml"]
