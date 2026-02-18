# -------------------------------------------------------------------------------
# S3 Orchestrator - Multi-Architecture Build and Push
#
# Author: Alex Freidah
#
# Go S3 orchestrator for unified S3-compatible storage access. Builds multi-arch
# container images.
# -------------------------------------------------------------------------------

REGISTRY   ?= registry.munchbox.cc
IMAGE      := s3-orchestrator
VERSION    ?= latest

FULL_TAG   := $(REGISTRY)/$(IMAGE):$(VERSION)
CACHE_TAG  := $(REGISTRY)/$(IMAGE):cache
PLATFORMS  := linux/amd64,linux/arm64

# -------------------------------------------------------------------------
# DEFAULT TARGET
# -------------------------------------------------------------------------

help: ## Display available Make targets
	@echo ""
	@echo "Available targets:"
	@echo ""
	@grep -E '^[a-zA-Z0-9_-]+:.*?## ' Makefile | \
		awk 'BEGIN {FS = ":.*?## "} {printf "  %-15s %s\n", $$1, $$2}'
	@echo ""

# -------------------------------------------------------------------------
# BUILDX SETUP
# -------------------------------------------------------------------------

builder: ## Ensure the Buildx builder exists
	@docker buildx inspect s3-orchestrator-builder >/dev/null 2>&1 || \
		docker buildx create --name s3-orchestrator-builder --driver-opt network=host --use
	@docker buildx inspect --bootstrap

# -------------------------------------------------------------------------
# BUILD (LOCAL)
# -------------------------------------------------------------------------

build: ## Build for local architecture
	@echo "Building $(FULL_TAG) for local architecture"
	docker build --build-arg VERSION=$(VERSION) -t $(FULL_TAG) .

# -------------------------------------------------------------------------
# BUILD AND PUSH (MULTI-ARCH)
# -------------------------------------------------------------------------

push: builder ## Build and push multi-arch images to registry
	@echo "Building and pushing $(FULL_TAG) for $(PLATFORMS)"
	docker buildx build \
	  --platform $(PLATFORMS) \
	  --build-arg VERSION=$(VERSION) \
	  -t $(FULL_TAG) \
	  --cache-from type=registry,ref=$(CACHE_TAG) \
	  --cache-to type=registry,ref=$(CACHE_TAG),mode=max \
	  --output type=image,push=true \
	  .

# -------------------------------------------------------------------------
# DEVELOPMENT
# -------------------------------------------------------------------------

generate: ## Generate sqlc query code
	sqlc generate

test: ## Run Go tests
	go test -race -v ./...

lint: ## Run Go linter
	@command -v golangci-lint >/dev/null 2>&1 || { echo "golangci-lint not installed"; exit 1; }
	golangci-lint run ./...

run: ## Run locally (requires config.yaml)
	go run ./cmd/s3-orchestrator -config config.yaml

# -------------------------------------------------------------------------
# INTEGRATION TESTS
# -------------------------------------------------------------------------

COMPOSE_FILE := docker-compose.test.yml

integration-deps: ## Start integration test dependencies (MinIO + PostgreSQL)
	docker compose -f $(COMPOSE_FILE) up -d minio-1 minio-2 postgres --wait
	docker compose -f $(COMPOSE_FILE) run --rm minio-setup

integration-test: integration-deps ## Run integration tests
	MINIO1_ENDPOINT=http://localhost:19000 \
	MINIO2_ENDPOINT=http://localhost:19002 \
	POSTGRES_HOST=localhost \
	POSTGRES_PORT=15432 \
	go test -race -v -tags integration -count=1 ./internal/integration/; \
	rc=$$?; $(MAKE) integration-clean; exit $$rc

integration-clean: ## Stop and remove integration test containers
	docker compose -f $(COMPOSE_FILE) down -v

# -------------------------------------------------------------------------
# CLEANUP
# -------------------------------------------------------------------------

clean: ## Remove build artifacts and local image
	go clean
	docker rmi $(FULL_TAG) || true

.PHONY: help builder build push generate test lint run integration-deps integration-test integration-clean clean
.DEFAULT_GOAL := help
