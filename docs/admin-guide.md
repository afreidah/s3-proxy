# S3 Orchestrator Admin Guide

This guide walks through deploying, configuring, and operating the S3 Orchestrator from scratch. For architecture and feature details, see the [README](../README.md). For client-side usage (AWS CLI, rclone, SDKs), see the [User Guide](user-guide.md).

## Prerequisites

- **PostgreSQL** — any recent version. The orchestrator auto-applies its schema on startup.
- **At least one S3-compatible storage backend** — OCI Object Storage, Backblaze B2, AWS S3, MinIO, Wasabi, etc. You need a bucket and access credentials on that backend.
- **The orchestrator binary** — either a Docker image (via `make build` or `make push`) or built from source (`make run`).

## Quickstart

Get a minimal single-bucket, single-backend orchestrator running in five steps.

### 1. Create a PostgreSQL database

```sql
CREATE USER s3orchestrator WITH PASSWORD 'changeme';
CREATE DATABASE s3orchestrator OWNER s3orchestrator;
```

The orchestrator creates its tables automatically on startup — no manual schema setup required.

### 2. Create a minimal config

```yaml
server:
  listen_addr: "0.0.0.0:9000"

buckets:
  - name: "my-files"
    credentials:
      - access_key_id: "AKID_MYFILES"
        secret_access_key: "changeme-replace-with-random-secret"

database:
  host: "localhost"
  port: 5432
  database: "s3orchestrator"
  user: "s3orchestrator"
  password: "changeme"
  ssl_mode: "disable"

backends:
  - name: "primary"
    endpoint: "https://namespace.compat.objectstorage.us-phoenix-1.oraclecloud.com"
    region: "us-phoenix-1"
    bucket: "my-actual-bucket"
    access_key_id: "backend-access-key"
    secret_access_key: "backend-secret-key"
    force_path_style: true
    quota_bytes: 21474836480  # 20 GB
```

Save this as `config.yaml`.

### 3. Start the orchestrator

```bash
# From source
s3-orchestrator -config config.yaml

# Or via Docker
docker run -v $(pwd)/config.yaml:/etc/s3-orchestrator/config.yaml -p 9000:9000 s3-orchestrator
```

### 4. Verify it's running

```bash
curl http://localhost:9000/health
# ok
```

### 5. Test with a quick upload/download

```bash
aws --endpoint-url http://localhost:9000 \
    s3 cp /etc/hostname s3://my-files/test.txt

aws --endpoint-url http://localhost:9000 \
    s3 cp s3://my-files/test.txt -
```

## Configuration Walkthrough

This section covers each config section in detail. See `config.example.yaml` for a complete template.

All config values support `${ENV_VAR}` expansion — the orchestrator calls `os.Expand` on the entire YAML file before parsing. Use this for secrets:

```yaml
database:
  password: "${DB_PASSWORD}"
```

### server

```yaml
server:
  listen_addr: "0.0.0.0:9000"    # required
  max_object_size: 5368709120     # 5 GB default
  backend_timeout: "30s"          # per-operation timeout for backend S3 calls
```

- `listen_addr` is the only required field.
- `max_object_size` caps single-PUT uploads. Larger objects should use multipart upload (most clients do this automatically).
- `backend_timeout` bounds individual S3 API calls to backends. Increase if you have slow backends or large objects.

### buckets

Each bucket defines a virtual namespace with one or more credential sets.

```yaml
buckets:
  - name: "app1-files"
    credentials:
      - access_key_id: "AKID_APP1"
        secret_access_key: "secret1"

  - name: "app2-files"
    credentials:
      - access_key_id: "AKID_APP2_WRITER"
        secret_access_key: "secret2"
      - access_key_id: "AKID_APP2_READER"
        secret_access_key: "secret3"
```

**Generating credentials:** Use `openssl rand` to produce random keys:

```bash
# Generate an access key ID (20 chars, uppercase + digits)
openssl rand -hex 10 | tr '[:lower:]' '[:upper:]'

# Generate a secret access key (40 chars, base64)
openssl rand -base64 30
```

**Validation rules:**
- Bucket names must not contain `/`.
- Bucket names must be unique across the config.
- Access key IDs must be globally unique across all buckets.
- Each bucket must have at least one credential set.
- Each credential needs either `access_key_id` + `secret_access_key` (SigV4) or `token` (legacy).

Multiple credentials on the same bucket let different services share a namespace with independent keys. This is useful when you want a writer service and a reader service accessing the same files.

### database

```yaml
database:
  host: "db.example.com"        # required
  port: 5432                     # default: 5432
  database: "s3orchestrator"     # required
  user: "s3orchestrator"         # required
  password: "${DB_PASSWORD}"
  ssl_mode: "require"            # default: disable — use "require" in production
  max_conns: 10                  # default: 10
  min_conns: 5                   # default: 5
  max_conn_lifetime: "5m"        # default: 5m
```

For production, always set `ssl_mode: require`. The default is `disable` for local development convenience.

Pool settings (`max_conns`, `min_conns`, `max_conn_lifetime`) control the pgx connection pool. The defaults are fine for most deployments. Increase `max_conns` if you're seeing connection wait times under high concurrency.

### backends

Each backend is an S3-compatible storage service with its own credentials and optional quota.

```yaml
backends:
  - name: "oci"
    endpoint: "https://namespace.compat.objectstorage.us-phoenix-1.oraclecloud.com"
    region: "us-phoenix-1"
    bucket: "my-oci-bucket"
    access_key_id: "${OCI_ACCESS_KEY}"
    secret_access_key: "${OCI_SECRET_KEY}"
    force_path_style: true
    quota_bytes: 21474836480     # 20 GB
```

**Endpoint URLs by provider:**

| Provider | Endpoint format | `force_path_style` |
|----------|----------------|-------------------|
| OCI Object Storage | `https://<namespace>.compat.objectstorage.<region>.oraclecloud.com` | `true` |
| Backblaze B2 | `https://s3.<region>.backblazeb2.com` | `true` |
| AWS S3 | `https://s3.<region>.amazonaws.com` | `false` |
| MinIO | `http://<host>:9000` | `true` |
| Wasabi | `https://s3.<region>.wasabisys.com` | `true` |

**Quota:** Set `quota_bytes` to limit how much data a backend can hold. Set to `0` or omit for unlimited. Quota is tracked in PostgreSQL and updated atomically with every write/delete.

**Usage limits:** Optional monthly caps on API requests, egress, and ingress per backend:

```yaml
    api_request_limit: 20000     # monthly API calls (0 = unlimited)
    egress_byte_limit: 1073741824  # 1 GB monthly egress (0 = unlimited)
    ingress_byte_limit: 0        # unlimited ingress
```

When a backend exceeds a usage limit, writes overflow to the next eligible backend. Limits reset each month automatically.

### telemetry

```yaml
telemetry:
  metrics:
    enabled: true
    path: "/metrics"             # default: /metrics
  tracing:
    enabled: false
    endpoint: "localhost:4317"   # OTLP gRPC endpoint
    insecure: true               # no TLS to collector
    sample_rate: 1.0             # 1.0 = trace everything
```

Metrics are served on the same port as the S3 API. Tracing exports spans via gRPC OTLP (e.g., to Tempo or Jaeger).

### circuit_breaker

The circuit breaker is always active. These settings tune its sensitivity.

```yaml
circuit_breaker:
  failure_threshold: 3           # consecutive DB failures before opening (default: 3)
  open_timeout: "15s"            # delay before probing recovery (default: 15s)
  cache_ttl: "60s"               # key→backend cache TTL during degraded reads (default: 60s)
```

When the database is unreachable, the orchestrator enters degraded mode: reads broadcast to all backends (with caching), writes return `503`. The circuit automatically recovers when the database comes back.

The defaults are sensible for most deployments. Increase `cache_ttl` if you have many read-heavy clients and want fewer backend round-trips during outages.

### rebalance

Moves objects between backends to optimize storage distribution. Disabled by default — enabling it will generate egress/ingress traffic on your backends.

```yaml
rebalance:
  enabled: true
  strategy: "pack"               # "pack" or "spread" (default: pack)
  interval: "6h"                 # default: 6h
  batch_size: 100                # objects per run (default: 100)
  threshold: 0.1                 # min utilization spread to trigger (default: 0.1)
```

- **pack** — fills backends in config order, consolidating free space onto the last backend. Good for maximizing free-tier allocations.
- **spread** — equalizes utilization ratios across all backends. Good for distributing load.

### replication

Creates additional copies of objects on different backends for redundancy.

```yaml
replication:
  factor: 2                      # copies per object (default: 1 = no replication)
  worker_interval: "5m"          # replication cycle (default: 5m)
  batch_size: 50                 # objects per cycle (default: 50)
```

The replication factor must be `<= number of backends`. The worker runs once at startup to catch up on any pending replicas, then continues at the configured interval. Reads automatically fail over to replicas if the primary copy is unavailable.

### rate_limit

Per-IP token bucket rate limiting. Requests exceeding the limit receive `429 SlowDown`.

```yaml
rate_limit:
  enabled: true
  requests_per_sec: 100          # token refill rate (default: 100)
  burst: 200                     # max burst size (default: 200)
```

## Multi-Backend Configurations

### Single backend with quota

The simplest setup. One backend with a byte cap:

```yaml
backends:
  - name: "oci"
    endpoint: "https://namespace.compat.objectstorage.region.oraclecloud.com"
    region: "us-phoenix-1"
    bucket: "my-bucket"
    access_key_id: "${OCI_KEY}"
    secret_access_key: "${OCI_SECRET}"
    force_path_style: true
    quota_bytes: 21474836480     # 20 GB
```

### Multiple backends with quotas (overflow routing)

Stack multiple free-tier allocations. When one backend fills up, writes overflow to the next:

```yaml
backends:
  - name: "oci-free"
    endpoint: "https://namespace.compat.objectstorage.region.oraclecloud.com"
    region: "us-phoenix-1"
    bucket: "free-tier-bucket"
    access_key_id: "${OCI_KEY}"
    secret_access_key: "${OCI_SECRET}"
    force_path_style: true
    quota_bytes: 21474836480     # 20 GB (OCI free tier)

  - name: "b2-free"
    endpoint: "https://s3.us-west-002.backblazeb2.com"
    region: "us-west-002"
    bucket: "free-tier-bucket"
    access_key_id: "${B2_KEY}"
    secret_access_key: "${B2_SECRET}"
    force_path_style: true
    quota_bytes: 10737418240     # 10 GB (B2 free tier)
```

This gives you 30 GB of combined storage across two providers.

### Multiple backends without quotas (requires replication)

When all backends are unlimited, you must set `replication.factor >= 2` so writes are distributed. Without quotas, there's no overflow routing — only the first backend would receive writes.

```yaml
backends:
  - name: "oci"
    endpoint: "https://namespace.compat.objectstorage.region.oraclecloud.com"
    region: "us-phoenix-1"
    bucket: "bucket-a"
    access_key_id: "${OCI_KEY}"
    secret_access_key: "${OCI_SECRET}"
    force_path_style: true
    # no quota_bytes — unlimited

  - name: "aws"
    endpoint: "https://s3.us-east-1.amazonaws.com"
    region: "us-east-1"
    bucket: "bucket-b"
    access_key_id: "${AWS_KEY}"
    secret_access_key: "${AWS_SECRET}"
    # no quota_bytes — unlimited

replication:
  factor: 2
```

**Validation rule:** You cannot mix unlimited and quota-limited backends. Either all backends have `quota_bytes` set (overflow routing) or all are unlimited (replication required).

## Onboarding a New Tenant

To add a new application to the orchestrator:

1. **Generate credentials** for the new tenant:

   ```bash
   echo "Access Key: $(openssl rand -hex 10 | tr '[:lower:]' '[:upper:]')"
   echo "Secret Key: $(openssl rand -base64 30)"
   ```

2. **Add a bucket entry** to your config:

   ```yaml
   buckets:
     # ... existing buckets ...
     - name: "new-app"
       credentials:
         - access_key_id: "${NEW_APP_ACCESS_KEY}"
           secret_access_key: "${NEW_APP_SECRET_KEY}"
   ```

3. **Restart the orchestrator** (or redeploy the Nomad job / Docker container).

4. **Hand the client** four pieces of information:
   - Endpoint URL (e.g., `http://s3-orchestrator.service.consul:9000`)
   - Bucket name (e.g., `new-app`)
   - Access Key ID
   - Secret Access Key

5. **Point them to the [User Guide](user-guide.md)** for client setup instructions.

## Importing Existing Data

The `sync` subcommand imports objects from an existing backend bucket into the orchestrator's metadata database. Use this when bringing a bucket that already has data under orchestrator management.

### Dry run first

Always preview what would be imported before committing:

```bash
s3-orchestrator sync \
  --config config.yaml \
  --backend oci \
  --bucket my-files \
  --dry-run
```

### Run the import

```bash
s3-orchestrator sync \
  --config config.yaml \
  --backend oci \
  --bucket my-files
```

The `--bucket` flag specifies which virtual bucket the imported objects belong to. Keys are stored internally as `{bucket}/{key}`, so this determines the namespace.

### Partial import with --prefix

Import only objects under a specific key prefix:

```bash
s3-orchestrator sync \
  --config config.yaml \
  --backend oci \
  --bucket my-files \
  --prefix "photos/"
```

Objects already tracked in the database for that backend are automatically skipped. The command logs per-page progress and a final summary.

### Sync flags

| Flag | Default | Description |
|------|---------|-------------|
| `--config` | `config.yaml` | Path to configuration file |
| `--backend` | (required) | Backend name to sync from |
| `--bucket` | (required) | Virtual bucket name to assign to imported objects |
| `--prefix` | `""` | Only sync objects with this key prefix |
| `--dry-run` | `false` | Preview without writing to the database |

## Monitoring

### Health endpoint

```bash
curl http://localhost:9000/health
```

Returns `ok` when the database is reachable, `degraded` when the circuit breaker is open. Always returns HTTP 200 (so the service stays in load balancer rotation during degraded mode).

### Key Prometheus metrics

If `telemetry.metrics.enabled` is `true`, metrics are exposed at `/metrics`. Key metrics to alert on:

| Metric | What to watch |
|--------|---------------|
| `s3proxy_quota_bytes_available{backend="..."}` | Alert when approaching 0 — backend is almost full |
| `s3proxy_circuit_breaker_state` | Alert when > 0 — database is unreachable (1=open, 2=half-open) |
| `s3proxy_replication_pending` | Alert when consistently > 0 — replicas are falling behind |
| `s3proxy_requests_total{status_code="5xx"}` | Alert on elevated 5xx rates |
| `s3proxy_degraded_write_rejections_total` | Writes being rejected due to degraded mode |
| `s3proxy_usage_limit_rejections_total` | Operations rejected by usage limits |

### Structured logs

All logs are JSON to stdout. Key fields: `msg`, `level`, `error`, `backend`, `operation`.

## Common Operations

### Adding a new backend

Add the backend to the `backends` list in your config and restart the orchestrator. Quota limits are synced to the database on startup.

### Adjusting quotas

Change `quota_bytes` in the config and restart. The orchestrator calls `SyncQuotaLimits` on startup, which updates the database to match the config.

### Enabling replication after initial setup

Add a `replication` section with `factor > 1` and restart. The replication worker runs immediately at startup to begin creating copies of existing objects, then continues at the configured interval.

Remember: the replication factor cannot exceed the number of backends.

### Rotating client credentials

Update the credentials in the bucket config and restart. The old credentials stop working immediately. Coordinate with the tenant to update their client configuration at the same time.

There is no graceful rotation mechanism — changing credentials requires a restart and brief coordination with the client.

## Docker Deployment

### Build the image

```bash
# Local build
make build

# Multi-arch build and push to registry
make push
```

### Run the container

```bash
docker run -d \
  --name s3-orchestrator \
  -v /path/to/config.yaml:/etc/s3-orchestrator/config.yaml \
  -p 9000:9000 \
  -e DB_PASSWORD=secret \
  -e OCI_ACCESS_KEY=... \
  -e OCI_SECRET_KEY=... \
  s3-orchestrator
```

The default entrypoint is `s3-orchestrator -config /etc/s3-orchestrator/config.yaml`. Mount your config file to that path, or override the command to use a different location.

Environment variables referenced in the config via `${VAR}` syntax are expanded at startup, so pass secrets as `-e` flags or via your orchestration platform's secret injection.

The `listen_addr` in your config determines which port the process binds to inside the container — make sure your `-p` mapping matches.
