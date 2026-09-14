# Loader health monitoring

The Java loader emits structured operational events that distinguish an empty
Tansu topic from a loader that is alive but not making durable progress. Health
events are written to standard output with this prefix:

```text
LOADER_HEALTH {"timestamp":"...","level":"INFO","target":"iceberg_loader::health","fields":{...}}
```

The prefix lets the Ops Agent route these records separately without changing
or duplicating the loader's existing plain-text commit and library logs. Events
never include credentials, raw Alpaca bars, exception messages, warehouse
paths, or object names.

On the VM, the version-controlled Ops Agent configuration routes these entries
to `gce_hadoop_catalog_loader_json`. It retains only records from
`iceberg-loader.service` whose journal `MESSAGE` starts with `LOADER_HEALTH `,
removes the prefix, parses the remainder, and copies `jsonPayload.level` to the
Cloud Logging `severity` field. The original health record is excluded from the
plain `gce_hadoop_catalog_journal` stream to avoid duplicates.

The resulting Cloud Logging payload keeps journald provenance alongside the
health contract:

```json
{
  "severity": "INFO",
  "jsonPayload": {
    "_SYSTEMD_UNIT": "iceberg-loader.service",
    "timestamp": "2026-09-13T15:01:49.599102174Z",
    "level": "INFO",
    "target": "iceberg_loader::health",
    "fields": {
      "event": "heartbeat",
      "state": "starting"
    }
  },
  "logName": "projects/PROJECT_ID/logs/gce_hadoop_catalog_loader_json"
}
```

The dashboard exporter queries this dedicated log name, exact systemd unit,
target, candidate topic, time lower bound, and only the event types required for
each calculation. It does not retrieve the general journal and filter it
locally. Separate bounded reads collect four recent heartbeats, 100 commit
boundary events (up to 50 complete commit pairs), and 20 classified failures.

## Essential values

| Value | Definition |
|---|---|
| `timestamp` | UTC time when this health event was emitted. |
| `process_started_at_utc` | UTC start time of the current Java process; a changed value identifies a restart. |
| `state` | Current loader activity inferred using the rules below. |
| `topic` | Tansu topic configured for this loader. |
| `partitions[].partition` | Kafka partition to which the accompanying offsets belong. |
| `partitions[].committed_offset` | Next Kafka source record durably acknowledged after an Iceberg commit. |
| `partitions[].end_offset` | Offset immediately after the newest source record currently in Tansu. |
| `partitions[].lag_records` | Durable partition lag: `end_offset - committed_offset`, never below zero. |
| `aggregate_lag_records` | Sum of durable lag over all assigned partitions. |
| `buffered_bars` | Decoded Alpaca bars held for the next bounded Iceberg append. |
| `buffered_source_records` | Distinct Kafka/WebSocket frames represented by the buffered bars. |
| `last_input_at_utc` | UTC time when this process last received a Kafka source record. |
| `last_commit_at_utc` | UTC time when this process last completed Iceberg and Kafka offset commits. |
| `last_commit_duration_ms` | Total duration of the last successful durable commit. |

One Kafka record represents one WebSocket frame and can contain multiple bars.
Consequently, `lag_records` and `buffered_bars` measure different things.

The public dashboard renames the Java event's camel-case partition members
(`committedOffset`, `endOffset`, and `lagRecords`) to snake case. It publishes
only the numeric offsets and partition number; the topic and raw event are not
included in `metrics.json`.

## State inference

The arrows below show evaluation priority, not a required lifecycle sequence:

```text
starting -> committing -> stalled -> catching_up -> buffering -> idle
```

- `starting`: catalog/table initialization, Kafka subscription, or initial
  offset discovery has not completed.
- `committing`: a batch is inside the durable write operation.
- `stalled`: lag is positive and the committed offset has not advanced for
  `LOADER_STALL_SECONDS`.
- `catching_up`: durable lag is greater than the Kafka records already
  represented in the in-memory batch.
- `buffering`: all outstanding source records are represented in the batch,
  which is waiting for its record or time threshold.
- `idle`: durable lag is zero and the batch is empty.

The stall clock starts when lag first becomes positive. It resets whenever a
committed offset advances or lag returns to zero. This prevents yesterday's
last commit from making the first new record of a session appear stalled.

The loader's partial-batch clock starts when the first bar enters an empty
batch, not when the process starts. It is cleared after a flush and restarted
for any remainder after a full bounded batch. A partial batch therefore waits
up to `LOADER_MAX_SECONDS` regardless of how long the loader was idle first.

## Events and failures

| Event or failure | Meaning |
|---|---|
| `heartbeat` | Periodic state and offset sample from the responsive consumer loop. |
| `commit_started` | The loader is about to write the reported bounded batch. |
| `commit_succeeded` | Iceberg and the corresponding Kafka offsets both committed. |
| `data_file_write_failed` | Parquet creation, serialization, upload, or close failed before an Iceberg commit. |
| `iceberg_commit_failed` | The data file may exist, but Iceberg did not commit it and Kafka offsets did not advance. |
| `iceberg_commit_state_unknown` | The client cannot prove whether Iceberg committed; Kafka offsets remain uncommitted. |
| `offset_commit_failed` | Iceberg committed but Kafka offsets did not, so an at-least-once replay is possible. |
| `loader_failed` | A fatal error occurred outside a classified durable-commit phase. |

Failures record only the exception class and these durability fields:

```text
iceberg_commit_status = committed | not_committed | unknown
offset_commit_status  = committed | not_committed | unknown
```

The application logs a classified failure before rethrowing it. The existing
systemd `Restart=on-failure` behavior remains responsible for process restart;
health monitoring does not restart the loader itself.

## Configuration

```text
LOADER_HEARTBEAT_SECONDS=60
LOADER_STALL_SECONDS=900
LOADER_MAX_POLL_INTERVAL_MS=900000
LOADER_MAX_RECORDS=1000
LOADER_MAX_SECONDS=300
LOADER_MAX_POLL_RECORDS=1
```

The 15-minute Kafka poll allowance prevents a known slow GCS/Iceberg commit
from immediately removing this single consumer from its group. A blocked call
cannot emit heartbeats from the same thread. Because the loader is a long-lived
service that can drain backlog outside market hours, five minutes without a
heartbeat is a warning and ten minutes is unhealthy in every market state. A
reported `stalled` state is unhealthy immediately. Commit freshness remains a
market-hours-only expectation.

## Commit timing boundaries

`last_commit_duration_ms` initially measures the entire durable operation:

```text
Parquet data-file write and close
-> Iceberg metadata/snapshot commit
-> Kafka offset commit
```

The next implementation checkpoint will split this into data-file write,
Iceberg metadata commit, Kafka offset commit, and total durable duration. These
are application-observed phase durations rather than individual GCS HTTP
request timings. Existing GCS connector high-latency warnings remain available
for request-level investigation without enabling verbose connector logging.

## Restart behavior

Health tracking and an unfinished batch are process memory and disappear on a
restart. Tansu SQLite retains topic records and consumer-group committed
offsets, while GCS retains committed Iceberg snapshots. A new loader process
therefore acquires the durable offset and reconstructs its health state; Cloud
Logging retains events from the previous process for diagnosis.
