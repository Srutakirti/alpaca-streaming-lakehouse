# wsr — Alpaca → Kafka extractor (Rust)

`wsr` connects to the Alpaca IEX market-data WebSocket, authenticates,
subscribes to bar data, and produces each WebSocket frame verbatim to a
Tansu/Kafka topic. It is a Rust implementation of the extractor role in the
larger `gcp_alpaca_datalake` pipeline — the loader (`load/subscriber.py`)
consumes from the same topic and appends to Iceberg, unchanged.

The wire format on the topic is identical to what any other producer in this
repo emits: each WS frame (a JSON **array** of bar objects) becomes one Kafka
message, payload = raw frame bytes, **no key**, single partition. Nothing
downstream needs to know which producer wrote the bytes.

Scope is local-first: it runs against a local Docker Tansu broker, reads all
configuration (including credentials) from environment variables, and depends
on **no GCP SDK**. A future Cloud Run deployment maps Secret Manager → env
vars at the platform boundary, so the binary itself stays credential-agnostic.

---

## Architecture

Three independent tokio tasks share one `Arc<Metrics>` of lock-free atomics. A
single `CancellationToken` is the shutdown signal; a signal-handler task flips
it on SIGINT or SIGTERM.

```
                          Bytes                         send_result
   ┌──────────┐   bounded mpsc(N)   ┌──────────────────┐  ──────────►  ┌────────┐
   │  ws::run │ ─────────────────►  │  producer_task   │               │ Kafka  │
   │ (main    │                     │  FuturesUnordered │ ◄── Delivery ─│ (Tansu)│
   │  fgnd)   │   +messages_        │  <DeliveryFuture> │     Future    └────────┘
   └────┬─────┘    received         └────────┬─────────┘
        │                                    │ poll completions
        │           ┌────────────────────────┴───────────────────────────────┐
        │           │            Arc<Metrics>  (atomics only — no locks)       │
        └──────────►│   read by metrics::emitter every METRICS_INTERVAL (JSON) │
                    └──────────────────────────────────────────────────────────┘

   Shutdown: signal-handler task selects over ctrl_c() / SIGTERM
             → CancellationToken.cancel() → every task observes it and exits.
```

Data flow per frame:

1. `ws::run` receives a text frame, bumps `messages_received`, touches
   `last_message_ts`, and `tx.send(bytes).await` into the bounded channel.
2. `producer_task` pulls from the channel and calls `producer.send_result(...)`,
   pushing the returned `DeliveryFuture` into a `FuturesUnordered` set.
3. As deliveries complete, the same task increments `messages_sent` (ack) or
   `delivery_failures` (error) from inside the `select!` — all in tokio context,
   no librdkafka callback thread.
4. `metrics::emitter` snapshots the atomics on a fixed cadence and logs one
   structured JSON line.

### Why this shape

- **Decoupled recv ↔ send via a bounded `mpsc::channel::<Bytes>`.** WS read
  timing is independent of Kafka send timing. If Kafka stalls, the channel
  fills and `tx.send().await` applies natural backpressure — the WS task pauses
  instead of buffering unboundedly inside librdkafka. Channel depth is an
  observable metric (`queue_depth`).
- **Pure-async delivery tracking, no callbacks.** The producer task owns a
  `FuturesUnordered<DeliveryFuture>` and updates metrics from the `select!` arm
  that polls completions. Delivery errors are real `Result`s in async code, so
  they carry full context (error variant, payload size) and never cross a
  thread boundary. A second `if inflight.len() < max_inflight` guard caps
  outstanding deliveries — a second backpressure layer on top of the channel.
- **Lock-free metrics.** Counters are `AtomicU64`, timestamps `AtomicI64`
  (epoch-millis), connection state `AtomicBool`. No `Mutex`/`RwLock`.
- **Typed errors at module boundaries via `thiserror`.** `ws::Error` and
  `kafka::Error` distinguish retryable from fatal; `main` uses `anyhow`.
- **Typed serde for the WS control protocol.** A `ControlFrame` struct replaces
  runtime `serde_json::Value` field-poking for the handshake.
- **One shutdown primitive.** `tokio_util::sync::CancellationToken`, cloned into
  every task. No `Arc<AtomicBool>`, no broadcast channel.

---

## Project layout

```
wsr/
├── Cargo.toml             # deps + edition 2024
├── rust-toolchain.toml    # pins toolchain to 1.85 (+ rustfmt, clippy)
├── PLAN.md                # the 7-checkpoint implementation plan (history)
├── README.md             # this file
├── secrets.txt            # local Alpaca creds — gitignored, never committed
└── src/
    ├── main.rs            # orchestrator + signal handling
    ├── config.rs          # env-var Config loader (+ secret redaction)
    ├── metrics.rs         # lock-free Metrics + emitter task
    ├── kafka.rs           # topic setup, producer build, producer_task
    └── ws.rs              # WebSocket connect/auth/subscribe + retry loop
```

---

## File-by-file

### `src/config.rs` — configuration

`Config` is a flat struct of 13 fields, all populated by `Config::from_env()`.
Three private helpers back it:

- `req(name)` — required var; returns `anyhow` error `missing env var <NAME>`.
- `opt(name, default)` — string var with a string default.
- `opt_parse::<T>(name, default)` — typed var (numbers); parse errors surface as
  `invalid <NAME>: <err>`.

`ALPACA_SYMBOLS` is parsed from a comma-separated list into `Vec<String>`,
trimming whitespace and dropping empties (default `["*"]` = all symbols).

A **custom `Debug` impl** redacts secrets so a `tracing::debug!(?cfg)` never
leaks credentials: `alpaca_secret` always renders as `<redacted>`;
`alpaca_key` shows only its first 4 chars (`PKCP…<redacted>`). This is the
reason `Config` derives `Clone` but **not** `Debug`.

`Config` is `Clone` because `producer_task` takes an owned copy (it needs
`max_inflight` after `main` moves on).

### `src/metrics.rs` — observability

`Metrics` holds only atomics, so any task can read/write without coordination:

| Field | Type | Meaning |
|---|---|---|
| `messages_received` | `AtomicU64` | frames pulled off the WS |
| `messages_sent` | `AtomicU64` | deliveries Kafka acked |
| `delivery_failures` | `AtomicU64` | deliveries that errored |
| `errors` | `AtomicU64` | transient WS/connect failures |
| `last_message_ts` | `AtomicI64` | epoch-ms of last WS frame |
| `last_delivery_ts` | `AtomicI64` | epoch-ms of last Kafka ack |
| `connection_status` | `AtomicBool` | WS currently connected |

`Metrics::new()` returns `Arc<Self>` directly so callers just `.clone()` the Arc
into each task. `snapshot()` renders a `serde_json::Value`, converting the
epoch-ms timestamps to RFC3339 strings (or `null` when zero) via
`chrono::DateTime::<Utc>::from_timestamp_millis`. Field names match the other
producers in this repo so the same dashboards work for any of them.

`emitter(metrics, interval, channel, shutdown)` spawns the reporting task:

- `tokio::time::interval` with `MissedTickBehavior::Skip`; the immediate first
  tick is consumed so the first emission lands one full `interval` in.
- If given the channel `Sender`, it adds two Rust-only fields:
  `queue_depth = max_capacity − capacity` and `queue_capacity`.
- `select!`s the tick against `shutdown.cancelled()` to exit promptly.

The emitter is generic over the channel item type (`T: Send + 'static`) so it
only depends on the `Sender`, not on `Bytes`.

### `src/kafka.rs` — Kafka producer

Typed boundary error:

```rust
pub enum Error {
    Kafka(#[from] KafkaError),   // client/admin failures
    Metadata(String),            // topic create / metadata fetch
}
```

Functions:

- **`ensure_topic(cfg)`** — Tansu does **not** auto-create topics. Builds an
  `AdminClient`, requests the topic (1 partition, replication factor 1), and
  treats `TopicAlreadyExists` as success. Any other code is a hard error.
- **`build_producer(cfg)`** — a `FutureProducer` with the **default**
  `ProducerContext` (no custom callback). Only `bootstrap.servers` is set;
  everything else is librdkafka default, matching the repo's wire contract.
- **`warm_metadata(producer, topic)`** — `fetch_metadata` to force the TCP
  handshake before the hot loop (the Rust equivalent of the Python
  `producer.list_topics(timeout=10)` warm-up).
- **`producer_task(...) -> JoinHandle<()>`** — the core. Owns a
  `FuturesUnordered<DeliveryFuture>` and runs one `biased` `select!` with a
  drain state machine:
  1. `shutdown.cancelled()` (while not draining) → flip to draining; stop
     accepting new work.
  2. `inflight.next()` (while non-empty) → on `Ok` bump `messages_sent` +
     `touch_last_delivery`; on `Err((err, msg))` bump `delivery_failures` and
     `warn!(error, payload_len)`; on oneshot `Canceled` bump
     `delivery_failures`. When draining and the set empties, break.
  3. `rx.recv()` (while not draining **and** `inflight.len() < max_inflight`) →
     enqueue; on `None` (channel closed) flip to draining.
  4. `else => break` — only reachable once every arm is disabled, i.e. drain
     finished.
- **`enqueue(...)`** — builds a keyless `FutureRecord` and calls
  `send_result`. A synchronous `QueueFull` is retried after a 50 ms backoff
  (librdkafka's internal queue is full); any other synchronous error bumps
  `delivery_failures`, logs, and drops the message.

The `biased` ordering means shutdown and completion-draining are always
serviced before pulling new work, so a cancel can't be starved by a busy
channel.

### `src/ws.rs` — WebSocket client + retry loop

Typed boundary error drives the retry decision:

```rust
pub enum Error {
    Transient(String),   // connect/IO/timeout/server-close → reconnect
    Fatal(String),       // server error frame / closed channel → give up
}
```

Components:

- **`ControlFrame`** — `Deserialize` struct for handshake frames (`T`, optional
  `msg`/`code`/`bars`). Replaces the old `serde_json::Value` poking. Data
  frames (bars) are **never** parsed here — they are forwarded as raw bytes.
- **`setup_ws(cfg)`** — connects, then walks the Alpaca handshake:
  `connected` greeting → send auth → expect `authenticated` → send subscribe →
  expect `subscription`. Returns the split `(writer, reader)`.
- **`expect(...)`** — awaits one control frame within the idle timeout.
  Classification is the key bit: a server `error` frame (bad credentials /
  config) becomes `Fatal`; a type/message mismatch or any IO/timeout becomes
  `Transient`.
- **`stream_frames(...)`** — the hot loop. A `biased` `select!` over:
  shutdown (close politely, return `Outcome::Shutdown`); the next WS message
  (text/binary → `forward`; ping/pong ignored — tungstenite auto-answers;
  close/error/`None` → `Transient`); and a per-iteration idle `sleep(timeout)`
  that resets on every received frame.
- **`forward(...)`** — counts the frame and `tx.send(bytes).await`. A closed
  channel means the producer task is gone → `Fatal`.
- **`run(...)`** — the public entry. A reconnect loop with exponential backoff
  (1s → ×2 → cap `BACKOFF_MAX`). Each attempt runs inside a
  `tracing::info_span!("ws_attempt", attempt)`. **A successful connect resets
  both the backoff and the attempt counter**, so `MAX_RETRIES` counts only
  *consecutive* failures. `Fatal` returns immediately; `Transient` backs off
  (interruptible by shutdown) until retries are exhausted.

The old inline Ctrl+C handling is gone — shutdown arrives only through the
token, so the WS code has a single exit path.

### `src/main.rs` — orchestrator

`#[tokio::main]` flow:

1. Init the `tracing-subscriber` JSON formatter; log level from `RUST_LOG`
   (default `info`).
2. `Config::from_env()`; `debug!(?cfg)` (redacted) + an `info!` startup line.
3. Kafka bring-up: `ensure_topic` → `build_producer` → `warm_metadata`.
4. Create `Arc<Metrics>`, a `CancellationToken`, and the bounded
   `mpsc::channel::<Bytes>(channel_capacity)`.
5. Spawn `metrics::emitter` and `kafka::producer_task`.
6. `spawn_signal_handler(token)` — a task that `select!`s `ctrl_c()` against a
   `SignalKind::terminate()` stream and cancels the token on the first
   (Cloud Run delivers **SIGTERM** on shutdown; a `#[cfg(not(unix))]` fallback
   handles Ctrl+C only).
7. `ws::run(...).await` on the foreground — it owns the lifetime.
8. Teardown on return: `token.cancel()` → drop `tx` (closes the channel) →
   `join!` the producer + emitter tasks → `producer.flush(10s)` → log the final
   metrics snapshot. The process exit code reflects the `ws::run` result
   (0 on clean shutdown, non-zero if it ended on a fatal/exhausted error).

### `Cargo.toml` / `rust-toolchain.toml`

- Edition **2024** → toolchain pinned to **1.85** (the minimum), plus `rustfmt`
  and `clippy`.
- `rdkafka` uses `["tokio", "cmake-build"]` — the `cmake-build` feature vendors
  and builds librdkafka, so the host needs `cmake`, `libssl-dev`,
  `libsasl2-dev`. `tokio-tungstenite` uses `native-tls` for the `wss://`
  endpoint.

---

## Configuration

All configuration is via environment variables. `RUST_LOG` is read directly by
`tracing-subscriber` and is **not** part of `Config`.

| Env var | Field | Default | Notes |
|---|---|---|---|
| `ALPACA_KEY` | `alpaca_key` | **required** | redacted in logs |
| `ALPACA_SECRET` | `alpaca_secret` | **required** | never logged |
| `ALPACA_WS_URI` | `ws_uri` | `wss://stream.data.alpaca.markets/v2/iex` | |
| `ALPACA_SYMBOLS` | `symbols` | `*` | comma-separated |
| `KAFKA_BROKER` | `kafka_broker` | `localhost:9092` | |
| `KAFKA_TOPIC` | `kafka_topic` | `alpaca-bars` | |
| `METRICS_INTERVAL` | `metrics_interval_secs` | `10` | seconds |
| `MAX_RETRIES` | `max_retries` | `5` | consecutive failures before giving up |
| `TIMEOUT` | `timeout_secs` | `120` | WS idle timeout |
| `BACKOFF_MAX` | `backoff_max_secs` | `120` | reconnect backoff cap |
| `CHANNEL_CAPACITY` | `channel_capacity` | `1024` | mpsc bound |
| `MAX_INFLIGHT` | `max_inflight` | `512` | in-flight delivery bound |
| `COMPONENT` | `component` | `alpaca-extractor-rs` | metrics label |
| `RUST_LOG` | — | `info` | tracing filter |

---

## Build & run

Prereqs for the vendored librdkafka build:

```bash
sudo apt-get install -y cmake libssl-dev libsasl2-dev
```

Start a local Tansu broker:

```bash
docker run -d --name tansu -p 9092:9092 ghcr.io/tansu-io/tansu:0.6.0 \
  --storage-engine memory:// \
  --kafka-listener-url tcp://0.0.0.0:9092 \
  --kafka-advertised-listener-url tcp://localhost:9092
```

Run the extractor:

```bash
cd wsr
export ALPACA_KEY=... ALPACA_SECRET=...
export KAFKA_BROKER=localhost:9092 KAFKA_TOPIC=alpaca-bars RUST_LOG=info
cargo run --release
```

Confirm frames landed (from the repo root):

```bash
python scripts/peek_kafka.py --broker localhost:9092 --topic alpaca-bars --from-beginning --max 20
```

> Bars only flow during US market hours (IEX feed). Off-market you will still
> see a clean `connected → authenticated → subscribed` handshake and periodic
> snapshots with `messages_received: 0`.

---

## Operational behavior

### Logging

Every line is structured JSON on stderr (Cloud Run auto-ingests). The emitter
logs one `metrics` event per `METRICS_INTERVAL`; its `snapshot` field is a
nested JSON object:

```json
{"component":"alpaca-extractor-rs","connection_status":true,
 "messages_received":0,"messages_sent":0,"delivery_failures":0,"errors":0,
 "last_message_ts":null,"last_delivery_ts":null,
 "queue_depth":0,"queue_capacity":1024}
```

### Shutdown

SIGINT (Ctrl+C) or SIGTERM cancels the token. The WS loop closes politely and
returns; the producer task drains its in-flight deliveries; the producer is
flushed (up to 10s); a final snapshot is logged; the process exits 0.

### Reconnect / backoff

Transient WS failures back off 1s → 2s → 4s → … capped at `BACKOFF_MAX`. A
successful connect resets the schedule and the attempt counter. After
`MAX_RETRIES` consecutive failures the process exits non-zero. The backoff
sleep is interruptible — shutdown during a backoff exits cleanly.

### Backpressure

Two layers: the bounded `mpsc` channel (WS `send().await` blocks when full) and
the bounded `FuturesUnordered` (producer stops pulling at `MAX_INFLIGHT`).
Memory stays bounded under a slow/stalled broker; watch `queue_depth` climb
toward `queue_capacity`.

---

## Security

- Credentials come **only** from `ALPACA_KEY` / `ALPACA_SECRET` env vars — no
  GCP SDK, no file reads in the binary. In Cloud Run, Terraform maps Secret
  Manager → these env vars at the platform boundary.
- `Config`'s custom `Debug` keeps secrets out of structured logs.
- `secrets.txt` holds local dev credentials. It is **gitignored** (via the repo
  root `.gitignore`) and must never be committed.

---

## Out of scope

- Dockerfile / Terraform module / Cloud Run job for `wsr` (not yet wired).
- GCP Secret Manager client inside the binary (handled at the Cloud Run edge).
- Per-bar produce or symbol-keyed partitioning — that would break the loader's
  one-frame-per-message input contract.

See `PLAN.md` for the checkpoint-by-checkpoint build history.
