# Plan: Turn `wsr/` into a Rust Kafka producer for Tansu

## Context

`wsr/` already does the hard half of the work: it connects to the Alpaca IEX WebSocket (`wss://stream.data.alpaca.markets/v2/iex`), authenticates, subscribes to all bars, and runs a `tokio::select!` recv loop with timeout + Ctrl+C handling. It uses `tokio-tungstenite` for WS and lists `rdkafka 0.36` in `Cargo.toml` — but `rdkafka` is **never instantiated**. Today the recv loop just `println!`s each frame (`wsr/src/main.rs:130`).

The goal: extend `wsr` into a real Kafka producer that streams Alpaca bars into Tansu, feeding the existing `load/subscriber.py` loader downstream. The wire format on the Kafka topic is fixed by what the loader expects (raw WS frame bytes verbatim, no key, single partition — see "Wire-format contract" below). Everything else is designed Rust-first: typed errors at module boundaries, bounded mpsc channels for backpressure, `bytes::Bytes` for zero-copy, lock-free atomic metrics, `tracing` spans for log correlation, tokio tasks coordinated by `CancellationToken`.

Scope is local-only — run against local Docker Tansu, no Dockerfile, no Terraform. A future Cloud Run deployment will map Secret Manager → env vars in Terraform, so the binary itself stays env-var-only with no GCP SDK dependency.

Also: `wsr/secrets.txt` has hardcoded Alpaca credentials. All of `wsr/` is untracked (`?? wsr/` in git status) so the secret was never committed. **Do not delete the file** — user wants to keep it locally. Add `secrets.txt` to `wsr/.gitignore` so it stays untracked permanently; no commit may ever include it.

## Wire-format contract (what the loader expects on the topic)

- Kafka producer config: only `bootstrap.servers` set; everything else librdkafka defaults.
- Tansu doesn't auto-create topics → `AdminClient::create_topics` (1 partition, rf=1) at startup, swallow `TopicAlreadyExists`, then `fetch_metadata` to force the TCP handshake.
- Each WS frame is produced as **one Kafka message**, payload = raw frame bytes verbatim, **no key**.
- Reconnect backoff: 1s → ×2 → cap `BACKOFF_MAX=120s`; reset on successful connect; `MAX_RETRIES=5`.
- Inner WS recv timeout: 120s → break inner loop, count as one retry.
- On SIGTERM/SIGINT: stop loops, `producer.flush(10s)`, log final metrics snapshot.
- Metrics shape per emission: `component`, `messages_received`, `messages_sent`, `delivery_failures`, `errors`, `last_message_ts`, `last_delivery_ts`, `connection_status`, `queue_depth`.

## Architecture — three tokio tasks + a shared `Arc<Metrics>`

```
       ┌────────────┐   Bytes    ┌──────────────────────────────┐  send_result   ┌───────────┐
       │  ws task   │ ─────────► │       producer task          │ ─────────────► │  Kafka    │
       └─────┬──────┘  mpsc(N)   │  ┌──────────────────────┐    │                └─────┬─────┘
             │                   │  │ FuturesUnordered<    │ ◄──┼─── DeliveryFuture ───┘
             │  +messages_       │  │   DeliveryFuture>    │    │
             │    received       │  └──────────┬───────────┘    │
             ▼                   │             │ poll completion ▼
       ┌─────────────────────────┴─────────────┴────────────────┴───────────────────┐
       │                  Arc<Metrics> (atomics only, no locks)                    │
       │     read by metrics emitter task every METRICS_INTERVAL (tracing::info)   │
       └──────────────────────────────────────────────────────────────────────────┘

       Shutdown: CancellationToken cancelled by signal handler task
                 (Ctrl+C OR SIGTERM) → all tasks observe & exit.
```

### Why this shape

- **Decoupled WS recv ↔ Kafka send via bounded `mpsc::channel::<Bytes>(1024)`.** WS read timing is independent of Kafka send timing. If Kafka stalls, the channel fills and `tx.send().await` provides natural backpressure — the WS task naturally pauses instead of buffering unboundedly inside librdkafka. Channel depth (`capacity - permits`) becomes an observable metric.
- **Pure-async delivery tracking, no callbacks.** Producer task uses `FuturesUnordered<DeliveryFuture>` and updates metrics from the `select!` arm that polls completions. Everything stays in tokio context — no `ProducerContext::delivery()` running on librdkafka's poller thread. Delivery errors are real `Result`s in async code, so they can be logged with full context (payload size, error variant) and could be escalated to the supervisor via a watch channel if a future change ever needs that. Bounded `FuturesUnordered` (via `if inflight.len() < max_inflight` guard) provides a *second* backpressure layer on top of the mpsc channel.
- **Lock-free metrics.** `AtomicU64` for counters, `AtomicBool` for `connection_status`, `AtomicI64` epoch-millis for timestamps. All writes happen from the producer task's `select!` arms (in tokio context) — no `Mutex`, no `RwLock`, no `ArcSwap`. Snapshot reads are fast enough to call from the emitter task without coordination.
- **Zero-copy payloads.** WS text frames are received as `String`; convert to `bytes::Bytes` once (cheap: `Bytes::from(text.into_bytes())` moves the buffer, no copy) and pass to Kafka via `FutureRecord::payload(&bytes)`.
- **Typed errors at module boundaries via `thiserror`.** `ws::Error` distinguishes `Transient(...)` (retry) from `Fatal(...)` (give up) so the retry loop matches on variants instead of an ad-hoc `ExitReason` enum. `kafka::Error` for startup failures. `main.rs` boundary uses `anyhow::Result`.
- **Typed serde structs for the WS protocol** (`auth`, `subscription`, `error` responses) — derive `Deserialize`, drop the runtime `serde_json::Value` field-poking of today's `expect_message` helper.
- **`tracing::info_span!("ws_attempt", attempt = n)`** wraps each connect+stream attempt — all logs inside auto-correlate. The emitter task emits a single `snapshot` field whose value is `serde_json::Value` from `Metrics::snapshot()`; the JSON formatter renders it as a nested object.
- **`tokio_util::sync::CancellationToken`** is the single shutdown signal — clones into each task; one signal-handler task selects over `ctrl_c()` and `SignalKind::terminate()` and calls `cancel()`. No `Arc<AtomicBool>` flag, no broadcast channel, no threading.Event analogue.

## Module breakdown — `wsr/src/`

| File | Public surface | Notes |
|---|---|---|
| `main.rs` | `#[tokio::main] async fn main() -> anyhow::Result<()>` | Init tracing JSON subscriber from `RUST_LOG`; load `Config::from_env()`; `kafka::ensure_topic(&cfg).await?`; build `Arc<Metrics>`; `let producer = kafka::build_producer(&cfg, metrics.clone())?;` `kafka::warm_metadata(&producer, &cfg.topic)?;` create `CancellationToken` + spawn signal-handler task; spawn `metrics::emitter(...)`; create `mpsc::channel::<Bytes>(cfg.channel_capacity)`; spawn `kafka::producer_task(...)`; **await `ws::run(...)` on the main task** so the WS retry loop owns the foreground; on return: `token.cancel()`, drop `tx`, `join_all(handles)`, `producer.flush(Duration::from_secs(10))`, final `tracing::info!(snapshot = %metrics.snapshot(), "final metrics")`. |
| `config.rs` | `pub struct Config`, `Config::from_env() -> anyhow::Result<Self>` | All fields listed below. Private helpers `req`/`opt`/`opt_u64`. No `config`/`figment`/`dotenvy` crate. |
| `ws.rs` | `pub async fn run(cfg: &Config, tx: mpsc::Sender<Bytes>, metrics: Arc<Metrics>, shutdown: CancellationToken) -> Result<(), Error>` + `pub enum Error` (`thiserror`) | Internal: `setup_ws`, `stream_frames`, typed `AuthResp`/`SubResp`/`ErrResp` serde structs. `Error::Transient { source }` (timeout / WS close / IO) → retry; `Error::Fatal { source }` (auth rejected / config / channel closed) → return immediately. Retry loop is a `loop { match setup_ws... }` with exponential backoff via `tokio::time::sleep`, backoff resets on successful connect, max retries from config. Each attempt wrapped in `tracing::info_span!("ws_attempt", attempt)`. On each successful recv: `bytes = Bytes::from(text.into_bytes())`, `metrics.touch_last_message()`, `metrics.messages_received.fetch_add(1, Relaxed)`, `tx.send(bytes).await.map_err(...)` — if channel closed (producer task gone), bubble as fatal. `tokio::select!` over `tx.send`, `framed.next()`, `tokio::time::sleep(timeout)`, and `shutdown.cancelled()`. The Ctrl+C handling at today's `wsr/src/main.rs:154-158` is removed — shutdown comes through the token. |
| `kafka.rs` | `pub fn build_producer(cfg) -> Result<FutureProducer, Error>`, `pub async fn ensure_topic(cfg) -> Result<(), Error>`, `pub fn warm_metadata(producer, topic) -> Result<(), Error>`, `pub fn producer_task(producer, topic, metrics, rx, shutdown) -> JoinHandle<()>` | Uses the **default `ProducerContext`** — no custom callback, no `MetricsContext`. `producer_task` owns a `FuturesUnordered<DeliveryFuture>` and runs one `tokio::select!` with four arms: (1) `rx.recv()` *guarded by* `if inflight.len() < cfg.max_inflight` — call `producer.send_result(FutureRecord::<(), [u8]>::to(topic).payload(&bytes))`; on synchronous `QueueFull` log + short sleep + retry; on success push the returned `DeliveryFuture` into the set; (2) `inflight.next()` *guarded by* `if !inflight.is_empty()` — on `Ok` increment `messages_sent` + `touch_last_delivery`, on `Err` increment `delivery_failures` + `tracing::warn!(%err, payload_len = ?, "delivery failed")`; (3) `shutdown.cancelled()` → stop accepting from `rx`, drain remaining `inflight` (still updating metrics), then break; (4) `rx.recv()` returning `None` (channel closed) → same drain-then-break. All metric updates and error logging happen in tokio context — no thread crossing. Two-layer backpressure: bounded `mpsc` channel + bounded `FuturesUnordered`. |
| `metrics.rs` | `pub struct Metrics { ... }` (atomics only), `Metrics::new() -> Arc<Self>`, `Metrics::snapshot(&self) -> serde_json::Value`, `touch_last_message`/`touch_last_delivery`, `pub fn emitter(metrics, interval, channel: Option<mpsc::Sender<_>>, shutdown) -> JoinHandle<()>` | `snapshot()` formats epoch-millis to ISO-8601 via `chrono::DateTime::<Utc>::from_timestamp_millis`. `emitter` uses `tokio::time::interval` with `MissedTickBehavior::Skip` and a `tokio::select!` on the tick vs `shutdown.cancelled()`. If `channel.is_some()`, snapshot includes `queue_depth = capacity - sender.capacity()`. |

## `Config` (all from env)

| Field | Env var | Default |
|---|---|---|
| `alpaca_key` | `ALPACA_KEY` | **required** |
| `alpaca_secret` | `ALPACA_SECRET` | **required** |
| `ws_uri` | `ALPACA_WS_URI` | `wss://stream.data.alpaca.markets/v2/iex` |
| `symbols` | `ALPACA_SYMBOLS` (CSV) | `*` |
| `kafka_broker` | `KAFKA_BROKER` | `localhost:9092` |
| `kafka_topic` | `KAFKA_TOPIC` | `alpaca-bars` |
| `metrics_interval_secs` | `METRICS_INTERVAL` | `10` |
| `max_retries` | `MAX_RETRIES` | `5` |
| `timeout_secs` | `TIMEOUT` | `120` |
| `backoff_max_secs` | `BACKOFF_MAX` | `120` |
| `channel_capacity` | `CHANNEL_CAPACITY` | `1024` |
| `max_inflight` | `MAX_INFLIGHT` | `512` |
| `component` | `COMPONENT` | `alpaca-extractor-rs` |

`RUST_LOG` is read directly by `tracing-subscriber`; not part of `Config`.

## `Cargo.toml` changes

```toml
[dependencies]
rdkafka = { version = "0.36", features = ["tokio", "cmake-build"] }   # was just ["tokio"]
tokio = { version = "1.52", features = ["full"] }
tokio-tungstenite = { version = "0.29", features = ["native-tls"] }
tokio-util = { version = "0.7", features = ["rt"] }
futures-util = "0.3"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
bytes = "1"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["json", "env-filter", "fmt"] }
anyhow = "1"
thiserror = "2"
chrono = { version = "0.4", default-features = false, features = ["clock"] }
```

System prereqs for the vendored librdkafka build: `cmake`, `libssl-dev`, `libsasl2-dev`. The explicit `cmake-build` feature documents the build path.

## Critical files

- `/home/kumararpita/gcp_alpaca_datalake/wsr/src/main.rs` — gutted; current handshake/recv code migrates to `ws.rs`.
- `/home/kumararpita/gcp_alpaca_datalake/wsr/src/config.rs` — **new**.
- `/home/kumararpita/gcp_alpaca_datalake/wsr/src/ws.rs` — **new** (receives existing handshake/recv code, rewritten for typed errors + channel send).
- `/home/kumararpita/gcp_alpaca_datalake/wsr/src/kafka.rs` — **new**.
- `/home/kumararpita/gcp_alpaca_datalake/wsr/src/metrics.rs` — **new**.
- `/home/kumararpita/gcp_alpaca_datalake/wsr/Cargo.toml` — deps above.
- `/home/kumararpita/gcp_alpaca_datalake/wsr/rust-toolchain.toml` — **new**, `channel = "1.85"` (minimum for edition 2024).
- `/home/kumararpita/gcp_alpaca_datalake/wsr/.gitignore` — **new**, ignore `target/`, `secrets.txt`, `.env`.
- `/home/kumararpita/gcp_alpaca_datalake/wsr/secrets.txt` — **keep on disk, do NOT delete**. Add to `wsr/.gitignore` so it's never committed. (Untracked today; user keeps it locally for convenience.)

## Implementation checkpoints

Each checkpoint is an atomic, committable unit. Status legend: `[ ]` not started · `[~]` in progress · `[x]` done. Update the status box and append the commit SHA as work proceeds.

---

### Checkpoint 1 — Repo hygiene

- **Status**: `[ ]`
- **Commit SHA**: _pending_
- **Commit message**: `chore(wsr): scaffold gitignore, toolchain, remove secrets file`
- **Files**: create `wsr/.gitignore` (ignore `target/`, `secrets.txt`, `.env`, `*.log`) — **do NOT delete `wsr/secrets.txt`**, just gitignore it; create `wsr/rust-toolchain.toml` (`channel = "1.85"`, components `rustfmt`, `clippy`).
- **Done when**: `cd wsr && cargo --version` succeeds with the pinned toolchain; `git status` no longer lists `secrets.txt` (the file still exists on disk but is gitignored); `wsr/.gitignore` + `wsr/rust-toolchain.toml` are untracked-but-present.

### Checkpoint 2 — Dependencies

- **Status**: `[ ]`
- **Commit SHA**: _pending_
- **Commit message**: `chore(wsr): add tracing, anyhow, thiserror, bytes, tokio-util, chrono, serde-derive`
- **Files**: `wsr/Cargo.toml` (full block in the Cargo.toml section above).
- **Done when**: `cd wsr && cargo build` compiles cleanly (slow first time due to vendored librdkafka — install `cmake libssl-dev libsasl2-dev` first if it fails).

### Checkpoint 3 — `Config`

- **Status**: `[ ]`
- **Commit SHA**: _pending_
- **Commit message**: `feat(wsr): env-based Config loader`
- **Files**: `wsr/src/config.rs` (new); `wsr/src/main.rs` (add `mod config;` and a temporary `let cfg = config::Config::from_env()?; tracing::info!(?cfg);` smoke check, gated on `RUST_LOG=debug`).
- **Done when**: with all required env vars set, `cargo run` prints the loaded `Config`; with `ALPACA_KEY` unset, it errors with a clear `missing env var ALPACA_KEY` message.

### Checkpoint 4 — `Metrics` + emitter task

- **Status**: `[ ]`
- **Commit SHA**: _pending_
- **Commit message**: `feat(wsr): lock-free Metrics + JSON snapshot emitter`
- **Files**: `wsr/src/metrics.rs` (new); `wsr/src/main.rs` (wire up `Metrics::new()`, spawn `metrics::emitter` with a short interval, manually bump counters from a test loop, run for ~30s, confirm snapshots in JSON logs).
- **Done when**: `cargo run` (with `RUST_LOG=info`) emits one structured JSON log per `metrics_interval_secs` containing the full metrics field set (including `queue_depth`).

### Checkpoint 5 — `kafka.rs` (standalone)

- **Status**: `[ ]`
- **Commit SHA**: _pending_
- **Commit message**: `feat(wsr): kafka producer task with FuturesUnordered delivery tracking`
- **Files**: `wsr/src/kafka.rs` (new). Uses default `ProducerContext` (no custom callback). `producer_task` owns a `FuturesUnordered<DeliveryFuture>`; metrics atomics are updated from the `select!` arm that polls completions. Throwaway test in `wsr/src/main.rs` creates the mpsc channel, spawns `producer_task`, pushes 10 fixed `"hello"` payloads through `tx`, drops `tx` to signal close, awaits the producer task, then logs a final snapshot.
- **Done when**: Tansu is running locally; `cargo run` produces 10 messages; `scripts/peek_kafka.py --max 10` returns them; the final snapshot shows `messages_sent: 10`, `delivery_failures: 0`. Induce a failure (point at a wrong port) and confirm `delivery_failures` increments and a `tracing::warn!` line appears per failed delivery.

### Checkpoint 6 — `ws.rs` (typed, channel-based)

- **Status**: `[ ]`
- **Commit SHA**: _pending_
- **Commit message**: `refactor(wsr): typed ws module with backoff + channel-based handoff`
- **Files**: `wsr/src/ws.rs` (new — receives existing handshake/recv code from current `main.rs:1–161`); strip the old code out of `wsr/src/main.rs`. Replace `serde_json::Value` poking with typed `AuthResp` / `SubResp` / `ErrResp` structs. Split into `setup_ws` + `stream_frames`. Implement `pub async fn run` with `thiserror`-derived `Error { Transient, Fatal }`, exponential backoff (reset on connect), `CancellationToken`, and `mpsc::Sender<Bytes>` outbound. Wrap each attempt in `tracing::info_span!("ws_attempt", attempt)`.
- **Done when**: `cargo clippy` clean; `cargo run` against Tansu connects, authenticates, subscribes, and sends frames into the channel; the channel-drain test from Checkpoint 5 (kept temporarily as a stub consumer) shows real Alpaca frames flowing through.

### Checkpoint 7 — `main.rs` orchestrator + graceful shutdown

- **Status**: `[ ]`
- **Commit SHA**: _pending_
- **Commit message**: `feat(wsr): main orchestrator with graceful shutdown + flush`
- **Files**: `wsr/src/main.rs` (final form: init tracing JSON, load config, `kafka::ensure_topic`, build `Arc<Metrics>`, build producer, `warm_metadata`, create `CancellationToken`, spawn signal-handler task (`ctrl_c` + `SIGTERM`), spawn `metrics::emitter`, create `mpsc::channel::<Bytes>(cfg.channel_capacity)`, spawn `kafka::producer_task`, `ws::run(...).await`, on return: `token.cancel()` → drop `tx` → `join_all(handles)` → `producer.flush(10s)` → final snapshot log).
- **Done when**: All verification steps (1–7) in the next section pass.

---

### Status table (single-glance summary — update inline)

| # | Checkpoint | Status | Commit |
|---|---|---|---|
| 1 | Repo hygiene | `[ ]` | _pending_ |
| 2 | Dependencies | `[ ]` | _pending_ |
| 3 | `Config` | `[ ]` | _pending_ |
| 4 | `Metrics` + emitter | `[ ]` | _pending_ |
| 5 | `kafka.rs` standalone | `[ ]` | _pending_ |
| 6 | `ws.rs` typed + channel | `[ ]` | _pending_ |
| 7 | `main.rs` + shutdown | `[ ]` | _pending_ |

## Verification

1. **Local Tansu**:
   ```bash
   docker run -d --name tansu -p 9092:9092 ghcr.io/tansu-io/tansu:0.6.0 \
     --storage-engine memory:// \
     --kafka-listener-url tcp://0.0.0.0:9092 \
     --kafka-advertised-listener-url tcp://localhost:9092
   nc -zv localhost 9092
   ```
2. **Run the Rust binary**:
   ```bash
   cd /home/kumararpita/gcp_alpaca_datalake/wsr
   export ALPACA_KEY=... ALPACA_SECRET=... KAFKA_BROKER=localhost:9092 KAFKA_TOPIC=alpaca-bars RUST_LOG=info
   cargo run --release
   ```
   Expect: structured JSON log lines; a `snapshot` event every 10s with `messages_received` and `messages_sent` climbing together, `connection_status: true`, `queue_depth` near 0.
3. **Confirm messages in Kafka** (other shell):
   ```bash
   python /home/kumararpita/gcp_alpaca_datalake/scripts/peek_kafka.py --broker localhost:9092 --topic alpaca-bars --from-beginning --max 20
   ```
   Each line should be a verbatim Alpaca WS frame (JSON array of bar events).
4. **End-to-end with the loader**: run `load/subscriber.py` against the topic and confirm rows land in the Iceberg warehouse.
5. **Shutdown**: Ctrl+C → one `final metrics` log line, exit 0. `kill -TERM <pid>` → same.
6. **Backoff sanity**: stop Tansu mid-run; observe `errors` increment, `connection_status: false`, sleep intervals roughly 1, 2, 4, 8, 16, 32, 64, 120, 120…; restart Tansu before retry 5 and confirm `backoff` resets on reconnect.
7. **Backpressure sanity**: induce slow consumer (run Tansu with a tiny memory budget or pause the broker container briefly); watch `queue_depth` rise toward `channel_capacity` and WS recv naturally slow — should NOT see unbounded memory growth.

## Out of scope (explicit)

- Dockerfile / Terraform module / Cloud Run job for `wsr`.
- GCP Secret Manager integration in the Rust binary (Terraform will map Secret Manager → env vars at the Cloud Run boundary later).
- Cloud Logging Rust client (stdout JSON is sufficient; Cloud Run auto-ingests).
- Per-bar produce / symbol-keyed partitioning (would break the loader's input contract; revisit if/when the consumer side changes too).
