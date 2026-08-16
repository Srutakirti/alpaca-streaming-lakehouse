# GCE HadoopCatalog pipeline

Checkpoint 1 provides a local, bounded synthetic pipeline. It uses Tansu with
SQLite for Kafka-compatible transport and a long-lived Java Iceberg-core loader
with `HadoopCatalog`; Spark is not in the write path. All timestamps are UTC.

## Local synthetic run

Build and validate the complete non-Spark flow with
`mvn -f loader-java/pom.xml package` followed by
`python scripts/run_local_stack.py`. It starts the official Tansu 0.6.0 Docker
container backed by SQLite, verifies that `flock` refuses a second writer,
publishes twelve Alpaca-compatible bar frames, and verifies the twelve raw
Iceberg records through a separate Java read-only client. The default runtime
directory is `.local-run/`.

For separate-process work, start the broker yourself, run
`scripts/run_local_loader.sh` once, then run `python scripts/run_synthetic_local.py`.
The producer only publishes to an already-running broker.

Docker must be running and have the `ghcr.io/tansu-io/tansu:0.6.0` image
available. The broker is configured with a SQLite storage URI, a loopback-only
listener, and no cloud credentials. Set `TANSU_IMAGE` to use a different
pre-pulled image. The raw table is append-only; `payload_hash` makes exact
Kafka replays explicit for downstream read views.

Run Python unit tests with `uv run pytest`, and Java tests with
`mvn -f loader-java/pom.xml test`. Kafka/Tansu acceptance validation starts the
Docker broker and the long-lived loader as separate processes.
