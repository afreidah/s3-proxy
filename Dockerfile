# -------------------------------------------------------------------------------
# S3 Proxy - Unified S3 Endpoint
#
# Project: Munchbox / Author: Alex Freidah
#
# Go-based S3 proxy service with Prometheus metrics and OpenTelemetry tracing.
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
    -ldflags="-s -w -X github.com/munchbox/s3-proxy/internal/telemetry.Version=${VERSION}" \
    -o s3-proxy ./cmd/s3-proxy

# -------------------------------------------------------------------------
# Runtime Image
# -------------------------------------------------------------------------

FROM alpine:3.21

RUN apk add --no-cache ca-certificates

COPY --from=builder /build/s3-proxy /usr/local/bin/

EXPOSE 8080

ENTRYPOINT ["s3-proxy"]
CMD ["-config", "/etc/s3-proxy/config.yaml"]
