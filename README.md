# GCE HadoopCatalog pipeline

Checkpoint 1 provides a local, bounded synthetic pipeline. It uses Tansu with
SQLite for Kafka-compatible transport and a long-lived Java Iceberg-core loader
with `HadoopCatalog`; Spark is not in the write path. All timestamps are UTC.

## Local synthetic run

Build and validate the complete non-Spark flow with
`mvn -f iceberg-loader-java/pom.xml package` followed by
`uv run python scripts/run_local_stack.py --source synthetic`. It starts the official Tansu 0.6.0 Docker
container backed by SQLite, verifies that `flock` refuses a second writer,
publishes twelve Alpaca-compatible bar frames, and verifies the twelve raw
Iceberg records through a separate Java read-only client. The default runtime
directory is a fresh temporary directory, so each acceptance run starts with an
empty Tansu SQLite database and Iceberg warehouse. Use `--runtime-dir PATH` or
`PIPELINE_RUNTIME_DIR=PATH` only when you intentionally want to retain state
for debugging.

The Java loader uses `ICEBERG_CATALOG_TYPE=hadoop` by default. It can also be
configured for `jdbc` or `rest` with `ICEBERG_CATALOG_URI`; producers and the
Kafka frame contract remain unchanged across catalog types.

For JDBC, provide the database driver on the loader classpath and optionally
set `ICEBERG_JDBC_DRIVER`, `ICEBERG_CATALOG_USER`, and
`ICEBERG_CATALOG_PASSWORD` (injected from a secret manager in cloud use).

To move an existing table to a new JDBC or REST catalog without rewriting
Parquet data, stop the single writer and run the included metadata-registration
tool with `ICEBERG_SOURCE_*` describing the current catalog and `ICEBERG_*`
describing the empty target catalog. It refuses to replace an existing target
table. The loader is then restarted only with the target catalog configuration.

Build the Rust extractor once with
`docker build -t gce-hadoop-catalog-websocket-extractor:local websocket-extractor-rust`, then exercise Alpaca's credentialed always-on test stream with:

```bash
set -a; source .env; source .env.local; set +a
uv run python scripts/run_local_stack.py --source fakepaca-wsr
```

It connects only to `wss://stream.data.alpaca.markets/v2/test` and subscribes
only to `FAKEPACA`.

For separate-process work, start the broker yourself, run
`scripts/run_local_loader.sh` once, then run `python scripts/run_synthetic_local.py`.
The producer only publishes to an already-running broker.

## Live local Fakepaca components

The scripts below run the broker, Java loader, and Rust Fakepaca extractor as
separate foreground processes. They share a temporary Tansu SQLite directory
and retain Iceberg data in `.local-notebook/warehouse`. Build the Java loader
and Rust image once, then use separate terminals in this order:

```bash
scripts/run_local_tansu.sh
scripts/ensure_local_fakepaca_topic.sh
scripts/run_local_fakepaca_loader.sh
scripts/run_local_fakepaca_extractor.sh
```

The extractor reads `ALPACA_KEY` and `ALPACA_SECRET` from uncommitted `.env`
and `.env.local`. Use `scripts/local_fakepaca_status.sh` to inspect the two
containers, Java loader process, and shared runtime values. Set `LOADER_MAX_RECORDS` or
`LOADER_MAX_SECONDS` before launching the loader to change its commit boundary.

Docker must be running and have the `ghcr.io/tansu-io/tansu:0.6.0` image
available. The broker is configured with a SQLite storage URI, a loopback-only
listener, and no cloud credentials. Set `TANSU_IMAGE` to use a different
pre-pulled image. The raw table is append-only; `payload_hash` makes exact
Kafka replays explicit for downstream read views.

Run Python unit tests with `uv run pytest`, and Java tests with
`mvn -f iceberg-loader-java/pom.xml test`. Kafka/Tansu acceptance validation starts the
Docker broker and the long-lived loader as separate processes.

## Read-only PySpark exploration

The checked-in notebook in `notebooks/` opens the same HadoopCatalog for local
or explicitly enabled GCS exploration. See `notebooks/README.md`; Spark is not
part of the loader or write path.
