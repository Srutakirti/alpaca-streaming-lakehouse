# VM pipeline services

The Docker-free VM runs five systemd services. Systemd manages process lifetime, restart behavior, dependencies, logs, and the restricted `gcehcatalog` runtime user. Application binaries and scripts are installed through the versioned release symlink:

```text
/opt/gce-hadoop-catalog/current
```

Persistent state and configuration are deliberately outside that release directory, so an application upgrade does not delete transport data or credentials:

```text
/var/lib/gce-hadoop-catalog/     Tansu SQLite state and loader lock
/etc/gce-hadoop-catalog/         runtime and extractor environment files
```

## Services

| Service | Purpose | Executable | Environment files |
|---|---|---|---|
| `tansu.service` | Local Kafka-compatible broker backed by SQLite. | `bin/tansu` | `runtime.env` |
| `tansu-topic.service` | Creates the configured topic if it does not exist. | `bin/ensure-topic` → Java `TopicProvisioner` | `runtime.env` |
| `iceberg-loader.service` | Consumes bars, batches them, and appends Iceberg data to GCS. | `scripts/run_loader.sh` → Java `Main` | `runtime.env` |
| `fakepaca-extractor.service` | Test-only Alpaca `/v2/test` WebSocket extractor. | `bin/wsr` | `runtime.env`, then `fakepaca.env` |
| `alpaca-extractor.service` | Limited direct-Alpaca WebSocket extractor. Started manually or by its weekday timer. | `bin/wsr` | `runtime.env`, then `alpaca.env` |
| `alpaca-extractor.timer` | Starts the direct extractor at 09:30 New York time on weekdays. | systemd timer | none |

For extractor services, the second environment file overrides any duplicate key in `runtime.env`.

## Dependency order

```text
Tansu broker
  → configured topic
    → Iceberg loader
      → optional Fakepaca or direct-Alpaca extractor
```

`tansu-topic.service` is a one-shot service. The loader and extractors are long-running. Extractors require the topic service, while the loader requires the topic service as well. Each service uses `Restart=on-failure`; systemd retries it after a crash.

## Weekday direct-Alpaca schedule

`alpaca-extractor.timer` uses `America/New_York`, so it follows the exchange's daylight-saving-time changes while the extractor records timestamps in UTC. It starts at 09:30 Monday through Friday. This intentionally simple schedule does not know US exchange holidays; on a holiday the extractor receives no bars and exits after its configured idle timeout.

After installing a release containing the timer, inspect the next trigger and enable it:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now alpaca-extractor.timer
systemctl list-timers alpaca-extractor.timer --all
```

The timer starts the complete dependency chain (Tansu, topic provisioner, loader, then direct extractor). To prevent the next scheduled start without changing any configuration, run:

```bash
sudo systemctl disable --now alpaca-extractor.timer
```

## Code entry points

- Tansu is the official pinned native Tansu binary. Its source is not part of this repository.
- `ensure-topic` starts [`TopicProvisioner.java`](../../iceberg-loader-java/src/main/java/io/gcehcatalog/loader/TopicProvisioner.java), which creates a one-partition topic if missing.
- `run_loader.sh` takes the advisory OS `flock` and starts [`Main.java`](../../iceberg-loader-java/src/main/java/io/gcehcatalog/loader/Main.java). It calls [`IcebergWriter.java`](../../iceberg-loader-java/src/main/java/io/gcehcatalog/loader/IcebergWriter.java) to write Parquet data and commit Iceberg snapshots.
- Both extractor services run the same Rust `wsr` binary from [`websocket-extractor-rust/`](../../websocket-extractor-rust/). Configuration determines whether it connects to Fakepaca or a direct Alpaca stream.

## Runtime configuration

`/etc/gce-hadoop-catalog/runtime.env` is shared by Tansu, the topic provisioner, and the loader. Important values are:

```text
KAFKA_BROKER             Tansu listener, normally 127.0.0.1:19092
KAFKA_TOPIC              Topic consumed by loader and produced by active extractor
KAFKA_GROUP_ID           Loader's durable consumer progress
ICEBERG_WAREHOUSE         GCS HadoopCatalog warehouse
ICEBERG_NAMESPACE         Destination Iceberg namespace
ICEBERG_TABLE             Destination Iceberg table
LOADER_MAX_RECORDS        Commit threshold by records
LOADER_MAX_SECONDS        Commit threshold by elapsed time
LOADER_MAX_POLL_RECORDS   Kafka records returned per poll; fixed at 1 for safe bounded source processing
LOADER_JAVA_OPTS          JVM sizing for the loader, normally -Xms128m -Xmx384m on the e2-micro
```

`/etc/gce-hadoop-catalog/fakepaca.env` and `alpaca.env` contain only extractor-specific values. They hold `ALPACA_KEY` and `ALPACA_SECRET`, which must never be committed, copied into a release archive, or printed in logs. Keep both files `root:gcehcatalog` with mode `0640`.

## Daily operations

```bash
# Inspect service health.
sudo systemctl status tansu iceberg-loader alpaca-extractor --no-pager

# Start or stop one service.
sudo systemctl start iceberg-loader.service
sudo systemctl stop alpaca-extractor.service

# Restart a service after editing the environment file it reads.
sudo systemctl restart iceberg-loader.service

# Follow live logs. Ctrl-C exits the log viewer, not the service.
sudo journalctl -u iceberg-loader.service -f

# Print recent logs without opening the interactive `less` pager.
sudo journalctl -u tansu.service -n 100 --no-pager
```

`--no-pager` prints output directly. Without it, systemd may open `less`; press `q` to exit that viewer.

## Memory and process checks

To see the memory currently charged to each relevant systemd service:

```bash
for unit in tansu tansu-topic iceberg-loader fakepaca-extractor alpaca-extractor; do
  printf '%-28s ' "$unit"
  sudo systemctl show "$unit.service" --property=MemoryCurrent --value
done
```

For a live system-wide cgroup view sorted by memory, use:

```bash
sudo systemd-cgtop --order=memory --iterations=1
```

`MemoryCurrent` may be empty for an inactive one-shot service such as `tansu-topic.service`; that is expected.

To view host-wide memory, including the total RAM and immediately available RAM, run:

```bash
free -h
```

The important columns are:

```text
total       Physical RAM assigned to the VM.
used        RAM currently used, including Linux filesystem cache.
available   RAM applications can use without swapping; this is the best free-memory indicator.
swap        Configured swap space and current usage.
```

For a machine-readable single-line summary:

```bash
free -b | awk '/^Mem:/ {printf "total=%.1f MiB used=%.1f MiB available=%.1f MiB\n", $2/1048576, $3/1048576, $7/1048576}'
```

## Cloud Logging

The VM uses Google Cloud Ops Agent to send selected systemd journal events to
Cloud Logging. The source template is
[`gce-hadoop-catalog-logging.yaml`](../../deployment/ops-agent/gce-hadoop-catalog-logging.yaml).
It avoids duplicate `/var/log/syslog` ingestion, drops unrelated host journal
traffic and Kafka telemetry-registration noise, and disables host metrics
collection to limit e2-micro overhead. The receiver retains pipeline units and
relevant PID-1 lifecycle events only. The complete, version-controlled YAML
must be used instead of copying a partial snippet.

This removes Ops Agent CPU, memory, disk, network, and process metrics from
Cloud Monitoring. Local memory inspection remains available through `free -h`
and `systemctl show`.

```bash
sudo systemctl status google-cloud-ops-agent-fluent-bit.service --no-pager
sudo journalctl -u google-cloud-ops-agent -n 100 --no-pager
```

## Safe release changes

Before installing a new release, stop the extractor first so no new bars arrive. Stop the loader and Tansu only for the short installation window. Kafka records remain in Tansu SQLite and the loader commits Kafka offsets only after an Iceberg snapshot succeeds, so a stopped loader resumes from its last committed position.

After the installer updates `/opt/gce-hadoop-catalog/current`, start services in dependency order and verify logs before starting an extractor. The approved weekday direct-Alpaca schedule is managed by `alpaca-extractor.timer`; keep Fakepaca unscheduled.
