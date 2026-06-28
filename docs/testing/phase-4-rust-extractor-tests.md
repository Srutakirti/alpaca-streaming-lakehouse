# Testing — Phase 4 (Rust extractor unit tests)

This documents the unit tests added to the Rust extractor (`wsr/`) in Phase 4 of
`TESTING_PLAN.md`, plus the two small behavior-preserving refactors that made the
logic testable without a network or Kafka.

Scope: pure, in-process unit tests via `#[cfg(test)]` modules. The extractor's real
WebSocket/Kafka I/O is **not** unit-tested (it needs live Alpaca creds + a broker);
that path is covered by the manual real-cred smoke check (below) and, indirectly, by
the loader integration / e2e tests that consume the frames it produces.

---

## How to run

```bash
cd wsr
cargo test            # 18 unit tests, no network/Kafka
cargo clippy --all-targets
cargo fmt --check
```

All three are the Rust quality gate; they pass with no Docker/GCP/credentials.

---

## What was added

| File | Refactor | Tests |
|---|---|---|
| `wsr/src/config.rs` | `from_env` → pure `from_getter(closure)` | defaults, missing-required, CSV symbols, overrides, invalid-number, redaction, Debug-no-leak |
| `wsr/src/ws.rs` | extract `classify_frame` + `FrameClass` from `expect` | connected/authenticated/subscription match; error→Fatal; type/msg mismatch; data-bar mismatch |
| `wsr/src/metrics.rs` | none | snapshot defaults, snapshot reflects atomic updates, `iso()` null/string |

No `[dev-dependencies]` were needed — the tests use only `serde_json` (already a dep)
and the standard library.

---

## Refactor 1 — `config.rs`: env access behind a closure

**Problem.** `Config::from_env()` read process env directly via `std::env::var`. To unit
-test defaults/overrides you'd have to mutate process-global env, which in **Rust edition
2024 is `unsafe`** (`set_var`/`remove_var` are not thread-safe) and would force every env
test to be serialized (e.g. with a `serial_test` dep + a global mutex) to avoid data
races with other threads.

**Fix.** Split the single env touch-point from the parsing logic:

```rust
pub fn from_env() -> Result<Self> {
    Self::from_getter(&|name| std::env::var(name).ok())   // the ONLY env access
}

fn from_getter(get: &dyn Fn(&str) -> Option<String>) -> Result<Self> { ... }
```

The helpers (`req`, `opt`, `opt_parse`) now take the `get` closure instead of calling
`std::env::var`. Production behavior is identical (`from_env` passes the real env
closure). Tests pass a `HashMap`-backed closure:

```rust
fn cfg_from(pairs: &[(&str, &str)]) -> Result<Config> {
    let map: HashMap<_, _> = pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
    Config::from_getter(&|name| map.get(name).cloned())
}
```

Result: config tests are **pure, parallel-safe, and need no `unsafe`** — a cleaner
outcome than the env-mutation approach the plan had allowed as a fallback.

### Config tests
- **defaults** — only the two required vars set → every optional field equals its
  documented default; `symbols == ["*"]`.
- **missing required** — absent `ALPACA_SECRET` → `Err` whose message names the var.
- **symbols CSV** — `"AAPL, TSLA , ,NVDA"` → `["AAPL","TSLA","NVDA"]` (trim + drop empties).
- **overrides** — set `KAFKA_BROKER`/`MAX_RETRIES`/etc → reflected (incl. numeric parse).
- **invalid number** — `METRICS_INTERVAL="not-a-number"` → `Err` naming the var.
- **redaction** — `redact("abc") == "<redacted>"` (≤4 chars); `redact("PKCP1234") ==
  "PKCP…<redacted>"`.
- **Debug never leaks** — `format!("{cfg:?}")` contains `<redacted>` and the truncated
  key prefix, but **not** the full key or the secret value.

## Refactor 2 — `ws.rs`: `classify_frame` extracted from `expect`

**Problem.** The handshake decision (is this frame the one we wanted? a fatal server
`error`? a transient mismatch?) was inline inside the async `expect()`, reachable only
with a live socket.

**Fix.** A pure function over a parsed frame:

```rust
#[derive(Debug, PartialEq, Eq)]
enum FrameClass { Match, Fatal(String), Mismatch(String) }

fn classify_frame(frame: &ControlFrame, want_type: &str, want_msg: Option<&str>) -> FrameClass
```

`expect()` now just does IO (read + timeout) and maps the result:
`Match → Ok`, `Fatal → Error::Fatal`, `Mismatch → Error::Transient`. Behavior is
unchanged. The error→Fatal vs mismatch→Transient distinction matters: `Fatal` aborts
the retry loop (bad creds/config), `Transient` triggers reconnect with backoff.

### ws tests
Tests build a `ControlFrame` by parsing a single JSON object with `serde_json` (also
exercises the `#[serde(rename = "T")]` mapping), then assert the classification:
- `success`/`connected`, `success`/`authenticated`, `subscription` (msg not required) →
  `Match`.
- `error` frame → `Fatal`, message includes the code (and `-1` when code absent).
- wrong type, or right type + wrong msg → `Mismatch`.
- a data **bar** frame (`T="b"`) seen during the handshake → `Mismatch`.

## metrics.rs tests
- **fresh snapshot** — correct `component`, all counters `0`, `connection_status=false`,
  timestamps render as JSON `null`.
- **reflects updates** — after `fetch_add` on counters, `store` on `connection_status`,
  and `touch_last_message()`, the snapshot shows the new values and a string timestamp
  for the touched field (and still `null` for the untouched one).
- **`iso()`** — `0 → null`; a real epoch-millis → an RFC3339 string.

---

## Testing patterns used (and why)

- **Dependency injection over global state** — passing an env getter makes config logic
  a pure function. Same idea as the loader's fixtures: isolate the one impure boundary,
  test everything else deterministically.
- **Separate decision from IO** — `classify_frame` is the branchy logic worth testing;
  pulling it out of the async `expect` lets it be tested with plain values, no runtime.
- **Test the security contract** — the redaction/Debug tests assert secrets never appear
  in log-facing output, an easy thing to regress.
- **No new dependencies** — kept the test surface to std + an existing dep.

## Not unit-tested (by design)

- `setup_ws` / `stream_frames` / the reconnect-backoff loop / the Kafka producer task —
  these are IO orchestration over real sockets/brokers. Covered by:
  - **Manual real-cred smoke** (market hours): export `ALPACA_KEY`/`ALPACA_SECRET` +
    `KAFKA_BROKER`, `cargo run --release`, verify frames via `scripts/peek_kafka.py` and
    rows via `scripts/query_iceberg.py`.
  - the loader integration + Phase 5 e2e tests, which consume the frame format the
    extractor produces.

## Files (paths relative to repo root)

Tests live inline in `#[cfg(test)] mod tests` blocks within each source file:
- `wsr/src/config.rs` — config defaults/errors/CSV/redaction
- `wsr/src/ws.rs` — `classify_frame` handshake classification
- `wsr/src/metrics.rs` — `snapshot()` + `iso()`

## Commit trail

| Commit subject | Phase |
|---|---|
| `test(wsr): unit-test config, frame classification, and metrics` | 4 |
