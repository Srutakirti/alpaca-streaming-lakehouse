# Local Fakepaca component testing

This runbook starts the local pipeline as independent components:

```text
Rust Fakepaca extractor -> Tansu topic -> Java Iceberg loader -> local Iceberg warehouse
```

The Rust extractor, Tansu, and Java loader each run in a separate terminal.
The extractor and loader are long-lived until interrupted with `Ctrl-C`.

## What persists

- Iceberg tables live in `.local-notebook/warehouse`. This is the data used by
  local exploration and is retained between runs.
- Tansu's Kafka-compatible SQLite state lives under `/tmp` by default. It is
  transport state, not Iceberg data.
- `.env` and `.env.local` supply Alpaca credentials and are never committed.

Every launcher sources `scripts/local_fakepaca_env.sh`. Its defaults are:

| Setting | Default | Purpose |
| --- | --- | --- |
| `KAFKA_BROKER` | `127.0.0.1:19092` | Tansu listener used by producer and loader |
| `KAFKA_TOPIC` | `alpaca-bars` | Alpaca-compatible bar topic |
| `ICEBERG_WAREHOUSE` | `.local-notebook/warehouse` | Persistent local HadoopCatalog warehouse |
| `LOADER_MAX_RECORDS` | `100` | Maximum bars per Iceberg commit |
| `LOADER_MAX_SECONDS` | `300` | Maximum seconds before a non-empty batch commits |

You normally invoke the launchers directly. Source the shared environment only
when inspecting or overriding settings:

```bash
source scripts/local_fakepaca_env.sh
```

## Prerequisites

Docker must be running. Build the Java JAR and Rust image once after a clean
checkout or relevant source change:

```bash
mvn -f iceberg-loader-java/pom.xml package
docker build -t gce-hadoop-catalog-websocket-extractor:local websocket-extractor-rust
```

Ensure `.env` and `.env.local` together provide `ALPACA_KEY` and
`ALPACA_SECRET`. The extractor launcher reads those files itself; do not paste
credentials into a command line.

## Step 1: start Tansu

In terminal 1:

```bash
scripts/run_local_tansu.sh
```

This starts the local Kafka-compatible broker with a loopback-only listener and
SQLite-backed transport state. It does not write Iceberg data. Leave the
terminal running. A healthy start includes:

```text
listening on: tcp://127.0.0.1:19092
ready in ...ms
```

## Step 2: create the topic

In terminal 2:

```bash
scripts/ensure_local_fakepaca_topic.sh
```

This creates `alpaca-bars` with one partition. It is safe to invoke again; it
does not create a second topic. Expected output:

```text
topic_ready=alpaca-bars
```

The command exits after the topic is ready.

## Step 3: start the Java loader

In terminal 2, after topic creation:

```bash
scripts/run_local_fakepaca_loader.sh
```

This starts the sole HadoopCatalog writer. It consumes Alpaca bar frames from
Tansu, writes append-only data to `alpaca.alpaca.bars_raw`, then commits Kafka
offsets only after the Iceberg commit succeeds. `flock` prevents a second
loader using the same runtime directory.

For a quick proof that commits are occurring, set a one-record boundary before
starting it:

```bash
export LOADER_MAX_RECORDS=1
scripts/run_local_fakepaca_loader.sh
```

For normal batching, omit that export and the loader commits at 100 bars or
five minutes. A successful commit prints:

```text
committed_at=... received=... inserted=...
```

Do not run a second loader or write to this HadoopCatalog from another client
while this loader is active.

## Step 4: start the Rust Fakepaca extractor

In terminal 3:

```bash
scripts/run_local_fakepaca_extractor.sh
```

This is the Rust WebSocket extractor. It loads local credentials, connects only
to `wss://stream.data.alpaca.markets/v2/test`, subscribes only to `FAKEPACA`,
and publishes received bars into `alpaca-bars` through Tansu. Leave it running.

Start it last so Tansu and the Java loader are ready to receive its records.

## Check component status

From any terminal:

```bash
scripts/local_fakepaca_status.sh
```

The command reports the Tansu and extractor containers, any Java loader
processes, the current warehouse, transport directory, and loader batch
settings. There must be only one intended Java loader writer for the warehouse.

## Shutdown and restart

Stop components in this order:

1. Press `Ctrl-C` in the Rust extractor terminal.
2. Press `Ctrl-C` in the Java loader terminal; let it flush a pending batch.
3. Press `Ctrl-C` in the Tansu terminal.

This stops processes only. It does not remove `.local-notebook/warehouse`.
For a fresh broker state without deleting Iceberg data, use a different
`LOCAL_FAKEPACA_TRANSPORT_DIR` consistently in each terminal before starting
the components.

## Common failures

- `ModuleNotFoundError: gce_hadoop_catalog` when creating the topic: update to
  the current branch and rerun `scripts/ensure_local_fakepaca_topic.sh`; the
  launcher sets `PYTHONPATH` itself.
- `Connection refused`: start Tansu first and wait for its `ready` line.
- `flock` failure: another loader is active; use the status script and stop the
  unwanted writer before starting a new one.
- Repeated Tansu `Storage(UnableToSend)` while Tansu should be active: its
  storage worker has failed. Stop the three components, select a new temporary
  transport directory, and restart from step 1. The Iceberg warehouse remains
  intact.
