# GCE HadoopCatalog pipeline

Checkpoint 1 provides a local, bounded synthetic pipeline. It uses Tansu with
SQLite for Kafka-compatible transport and Spark's Iceberg `HadoopCatalog` for
the local warehouse. All application timestamps are UTC.

## Local synthetic run

`python scripts/run_synthetic_local.py` starts the official Tansu 0.6.0 Docker
container, publishes a fixed set of
synthetic bars, loads them into the local Iceberg warehouse, and prints the
row count. The default runtime directory is `.local-run/`, which is ignored.

Docker must be running and have the `ghcr.io/tansu-io/tansu:0.6.0` image
available. The broker is configured with a SQLite storage URI, a loopback-only
listener, and no cloud credentials. Set `TANSU_IMAGE` to use a different
pre-pulled image; set `ICEBERG_SPARK_RUNTIME_JAR` to an Iceberg Spark runtime
JAR if automatic Spark package resolution is not available.

Run unit tests with `python -m pytest`. Run local-service validation with
`python -m pytest -m integration`; integration tests start Spark and, where a
runnable binary is available, a Tansu broker.
