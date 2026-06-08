# Runbook: Loader crash-loop from Iceberg catalog/warehouse drift

**Last incident:** 2026-04-28 — production `alpaca-loader` Cloud Run service.

## Symptoms

- `alpaca-loader` Cloud Run service crash-loops at startup. Cloud Run keeps restarting it
  via `MANUAL_OR_CUSTOMER_MIN_INSTANCE` because `min-instances >= 1`.
- Container logs show a `FileNotFoundError` raised from
  `bootstrap_iceberg()` → `catalog.table_exists()` → `pyiceberg.catalog.sql.load_table()` →
  `pyarrow.fs.open_input_file()`:

  ```
  google::cloud::Status(NOT_FOUND: ... No such object:
  <warehouse>/alpaca/bars/metadata/<NNNNN>-<uuid>.metadata.json ...
  http_status_code=404)
  ```
- Producers (extractor or `scripts/peek_kafka.py` running in producer mode) are healthy;
  Tansu has fresh messages on `alpaca-bars`. Consumer group `alpaca-iceberg-loader` makes
  no progress because the loader process never reaches its `consumer.poll()` loop.

## Root cause

The Postgres catalog (Cloud SQL `alpaca-iceberg-catalog`, db `iceberg`, table
`iceberg_tables`) holds a row pointing `alpaca.bars` at a metadata file that no longer
exists in the GCS warehouse bucket. The two have drifted: the warehouse was wiped (or the
specific metadata object deleted) without removing the catalog row.

`bootstrap_iceberg` in `load/subscriber.py:166` calls `catalog.table_exists(full_name)`,
which under PyIceberg's SqlCatalog is implemented as "load the table and see if it
opens" — so a missing metadata file raises `FileNotFoundError` instead of returning
`False`, and the loader exits before `create_table()` can run.

## Fix

If the warehouse contents are truly gone (verify first), delete the orphaned catalog row
so the next loader startup recreates the table cleanly via `catalog.create_table()`.

### 1. Confirm the warehouse is empty for that table

```bash
gcloud storage ls -r gs://<warehouse-bucket>/alpaca/bars/ 2>&1
# Expect: "ERROR: ... matched no objects"  → safe to drop the catalog row.
# If objects exist, do NOT delete the row — instead rewind metadata_location to the
# newest existing metadata file (manual catalog edit). That path is not covered here.
```

### 2. Connect to Cloud SQL via cloud-sql-proxy

The laptop is IPv6-only and Cloud SQL `authorized_networks` is IPv4 only, so the proxy is
required (per `CLAUDE.md`).

```bash
# Install proxy if missing
curl -sSLo /tmp/cloud-sql-proxy \
  https://storage.googleapis.com/cloud-sql-connectors/cloud-sql-proxy/v2.11.0/cloud-sql-proxy.linux.amd64
chmod +x /tmp/cloud-sql-proxy

# Background the proxy
/tmp/cloud-sql-proxy <PROJECT_ID>:us-east1:alpaca-iceberg-catalog --port 5432 &
```

### 3. Inspect, then delete the orphan row

```bash
PG="PGPASSWORD=$(gcloud secrets versions access latest --secret=ICEBERG_DB_PASSWORD) \
    psql -h 127.0.0.1 -U iceberg -d iceberg"

# Inspect (note the catalog_name — pyiceberg defaults to whatever was passed to SqlCatalog;
# this project uses "alpaca_catalog", set in load/subscriber.py:159).
$PG -c "SELECT catalog_name, table_namespace, table_name, metadata_location
        FROM iceberg_tables;"

# Delete (one row)
$PG -c "DELETE FROM iceberg_tables
        WHERE catalog_name='alpaca_catalog'
          AND table_namespace='alpaca'
          AND table_name='bars';"
```

### 4. Bounce the loader

A no-op env-var change is the simplest way to force a new revision:

```bash
gcloud run services update alpaca-loader --region=us-east1 \
  --update-env-vars=_BOUNCE=$(date +%s) --quiet
```

### 5. Verify recovery

`bootstrap_iceberg()` will write a fresh `00000-<uuid>.metadata.json` to GCS and reinsert
the catalog row. The first call against an empty GCS prefix can take ~2 minutes — do not
panic if logs are quiet between "Kafka topic ready" and "Iceberg table ready".

```bash
# Should now show ONE log line "Iceberg table ready: alpaca.bars @ gs://..." per startup
gcloud logging read \
  'resource.labels.revision_name="<new-revision>"' \
  --limit=20 --freshness=10m

# New metadata file exists
gcloud storage ls gs://<warehouse-bucket>/alpaca/bars/metadata/

# Catalog row repopulated, pointing at 00000-...metadata.json
$PG -c "SELECT table_namespace, table_name, metadata_location FROM iceberg_tables;"
```

After the first successful `flush()` (BATCH_SIZE=100 records OR BATCH_INTERVAL=300s with
records present), `consumer_lag` in the periodic metrics log should drop toward zero.

## Prevention

The catalog ↔ warehouse pair must be treated as a single durable unit. Anything that
modifies one without the other reproduces this incident.

- **Never** wipe `gs://<warehouse>/alpaca/` without also clearing the matching row from
  `iceberg_tables`. Same in the other direction.
- If you need a clean slate during development, do both atomically:
  ```bash
  gcloud storage rm -r gs://<warehouse-bucket>/alpaca/
  $PG -c "TRUNCATE iceberg_tables, iceberg_namespace_properties;"
  ```
- Confirm the GCS bucket has **no lifecycle rules** that delete metadata objects
  (`gcloud storage buckets describe <warehouse-bucket> --format='value(lifecycle)'`).
- If the wipe was unintentional, check Cloud Audit Logs for `storage.objects.delete` on
  the `metadata/` prefix to find the actor (terraform, manual gcloud, lifecycle policy).

## Related code / config

- `load/subscriber.py:151` — `bootstrap_iceberg()` (the failing call site).
- `load/subscriber.py:159` — `SqlCatalog("alpaca_catalog", ...)` — this string is the
  `catalog_name` written to the Postgres row. Match it in the `DELETE` exactly.
- `terraform/modules/catalog/` — provisions the Cloud SQL instance, db, user, and
  password secret (`ICEBERG_DB_PASSWORD`).
- `terraform/modules/warehouse/` — provisions the GCS warehouse bucket.
- `CLAUDE.md` — Cloud SQL Auth Proxy and IPv6 notes.
