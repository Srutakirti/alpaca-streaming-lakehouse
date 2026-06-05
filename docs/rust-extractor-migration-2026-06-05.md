# Rust Extractor Migration + Loader CPU Fix — 2026-06-05

Migration of the Alpaca→Kafka producer from the Python extractor to the Rust
`wsr` producer on GCP, plus a root-caused fix for slow Iceberg appends in the
loader. Validated end-to-end against live market data.

---

## 1. Summary

| Area | Before | After |
|---|---|---|
| Extractor (producer) | `alpaca-extractor` (Python, `extract/extractor.py`) | `alpaca-extractor-rs` (Rust, `wsr/`) |
| Alpaca credentials | fetched at runtime via GCP Secret Manager **SDK** | injected from Secret Manager into **env vars** (Rust has no GCP SDK) |
| Loader append latency | **~90–150 s** per flush (CPU-throttled) | **~1.2 s** per flush (CPU always allocated) |
| Probe job | `alpaca-extractor` image + `probe.py` | **unchanged** (still Python image) |
| Loader / frontend code | — | **unchanged** |

The wire contract is identical (one WebSocket frame → one keyless Kafka
message), so `load/subscriber.py` consumes from the Rust producer without any
change. The only behavioral difference between producers is the implementation;
the bytes on the topic are the same.

---

## 2. Why

`wsr/` was a feature-complete Rust port of the extractor (typed errors, bounded
mpsc backpressure, lock-free atomic metrics, `FuturesUnordered` delivery
tracking, `CancellationToken` shutdown) but was never wired into Docker or
Terraform. Goal: run it in place of the Python extractor on GCP with everything
else unchanged, and prove data flows API → Tansu → loader → Iceberg → frontend.

---

## 3. What changed

### 3.1 New container image: `alpaca-extractor-rs`
- **`wsr/Dockerfile`** — multi-stage build. Build stage (`rust:bookworm`) installs
  `cmake libssl-dev libsasl2-dev` for the vendored librdkafka (`rdkafka`
  `cmake-build` feature); runtime stage (`debian:bookworm-slim`) ships
  `ca-certificates libssl3 libsasl2-2` + the static `wsr` binary. Final image
  ~90 MB. Build context is `wsr/` (standalone Cargo project).
- **`wsr/.dockerignore`** — excludes `target/`, `secrets.txt`, `.env`, `*.log`
  (keeps credentials out of image layers).
- **`scripts/build_and_push.sh`** — builds + pushes `alpaca-extractor-rs`
  alongside the existing three images. The Python `alpaca-extractor` image is
  still built because the probe job depends on it.

### 3.2 Extractor Terraform — `terraform/modules/extractor-job/main.tf`
- Image → `${image_base}/alpaca-extractor-rs:${image_tag}`.
- Removed `GCP_PROJECT_ID` and `LOG_MODE` (Rust uses neither).
- Added `RUST_LOG=info`; kept `KAFKA_BROKER`, `KAFKA_TOPIC`, `ALPACA_SYMBOLS`,
  `METRICS_INTERVAL`, `MAX_RETRIES`, `TIMEOUT`.
- **Secret Manager → env vars** via Cloud Run v2 `value_source { secret_key_ref }`
  for `ALPACA_KEY` and `ALPACA_SECRET` (secrets already existed in Secret
  Manager). The extractor SA already had `roles/secretmanager.secretAccessor`,
  so no IAM change was needed.

### 3.3 Loader CPU-throttling fix — `terraform/modules/loader-service/main.tf`
Added a `resources` block with `cpu_idle = false` (CPU always allocated) and
`startup_cpu_boost = true`. See §5 for the full root-cause analysis.

### 3.4 Synthetic cleanup utility — `scripts/delete_synthetic.py`
Row-level delete of synthetic test rows from the Iceberg table by symbol
(`pyiceberg table.delete(delete_filter=In("S", symbols))`). CLI-parameterized
(default `ZZSYNTH1`/`ZZSYNTH2`), `--dry-run`/`--yes`/`--prod`. Mirrors how
`scripts/query_iceberg.py` loads the catalog. Not needed in the end (real market
data was used for E2E) but kept for future off-hours testing.

---

## 4. Deployment & validation

### Phase 1 — local retest (sqlite + local FS)
- **Handshake:** `wsr` against local Docker Tansu → `connected → authenticated →
  subscribed → streaming`; clean SIGTERM drain, exit 0.
- **Data flow:** synthetic `ZZSYNTH*` bars → local loader → Iceberg (232 rows,
  0 errors), confirmed with `scripts/query_iceberg.py`.
- **Cleanup tool:** `delete_synthetic.py --dry-run` counted 232, `--yes` removed
  them, re-query → 0 rows.

### Phase 2/3 — build, push, deploy
- Built + pushed `alpaca-extractor-rs:latest` (digest `bf77ab59…`).
- **Scoped** `terraform apply -target=module.extractor_job` (1 changed) to avoid
  pulling in unrelated pre-existing drift in other modules.
- Verified deployed job: Rust image, the 7 plain env vars, and `ALPACA_KEY`/
  `ALPACA_SECRET` as secret refs; no `GCP_PROJECT_ID`/`LOG_MODE`.
- Manual run during market hours → handshake confirmed in Cloud Logging;
  `connection_status: true`. Successful `authenticated` proved the Secret
  Manager → env-var injection.

### Phase 4 — end-to-end with real market data (10:24 ET)
- Extractor metrics: `messages_received = messages_sent = 4`, `delivery_failures
  = 0`, real timestamps climbing.
- Frontend `/api/bars?symbol=AAPL` returned a same-session bar
  (`2026-06-05T14:17:00Z`), i.e. data traversed the full chain:
  **Rust extractor → Tansu VM → loader → Iceberg → frontend.**
- Because real data was used, no synthetic injection or cleanup was required.

---

## 5. Loader slow-append investigation (root cause)

**Symptom:** after the Rust extractor began producing, loader metrics showed
`last_flush_duration_ms` of **90,000–150,000 ms** for appends of only 2–4
records, with `consumer_lag` creeping up and a single stuck
`iceberg_append_errors: 1`.

**Hypotheses tested and rejected:**
- *Metadata bloat* — the GCS warehouse had only **63 snapshots / 64 data files**;
  a 63-snapshot `metadata.json` is tiny. Rejected.
- *Retry/backoff inflating the timer* — `flush()` in `load/subscriber.py` times
  **only** `iceberg_table.append()` and sets the duration on success; there is no
  retry/sleep in that path. Rejected.
- *The Rust swap* — the extractor is a Cloud Run **Job** (always full CPU) and
  its own metrics were perfect; the wire bytes are identical to Python. Rejected.

**Root cause:** the loader Cloud Run **Service** had
`run.googleapis.com/cpu-throttling = true` (the default). Cloud Run only
allocates CPU to a service **while it is handling an HTTP request**. The loader
is a background Kafka consumer that calls `iceberg_table.append()` (pyarrow
Parquet encoding + Avro manifest writes + GCS I/O) **inside its poll loop,
outside any request** — so that work ran on throttled CPU. This produced the
~1,600× slowdown observed (≈60 ms locally → ≈100 s in prod).

**Fix:** set `cpu_idle = false` on the loader container (CPU always allocated for
the always-on `min=max=1` instance). `terraform apply -target=module.loader_service`.

**Result:**

| Metric | Before | After |
|---|---|---|
| `last_flush_duration_ms` | 90,000–150,000 | **~1,200** |
| `iceberg_append_errors` | 1 (stuck) | **0** |
| `consumer_lag` | creeping up | low & stable |

~1.2 s is healthy for an Iceberg commit to GCS + Cloud SQL (the residual vs the
local ~60 ms is network round-trips). The one-time append error did not recur.

**Lesson:** any Cloud Run **Service** that does work outside HTTP request scope
(background consumers, schedulers, queue workers) must set `cpu_idle = false`.
Cloud Run **Jobs** are unaffected — they always get full CPU.

---

## 6. Files changed

| File | Change |
|---|---|
| `wsr/Dockerfile` | new — multi-stage Rust build |
| `wsr/.dockerignore` | new — exclude `target/`, `secrets.txt` |
| `scripts/build_and_push.sh` | add `alpaca-extractor-rs` build + push |
| `terraform/modules/extractor-job/main.tf` | Rust image + secret env + env cleanup |
| `terraform/modules/loader-service/main.tf` | `cpu_idle=false`, `startup_cpu_boost=true` |
| `scripts/delete_synthetic.py` | new — synthetic row cleanup utility |

---

## 7. Operational notes

- **Rollback (extractor):** point `module.extractor_job` image back to
  `alpaca-extractor` and restore its env (Python reads creds from the SM SDK, so
  drop the `secret_key_ref` env and re-add `GCP_PROJECT_ID`/`LOG_MODE`), then
  `terraform apply`.
- **Rollback (loader):** remove the `resources` block (or set `cpu_idle = true`)
  and `terraform apply`. Only do this if reverting; throttling is the bug.
- **Probe job** still uses the Python `alpaca-extractor` image (`probe.py`), so
  that image and `extract/Dockerfile` must keep being built.
- **Lifecycle:** the scheduler starts the extractor at 08:00 ET and stops it at
  17:00 ET on weekdays. Alpaca IEX only emits bars during US market hours, so a
  successful off-hours run shows `connection_status: true` with
  `messages_received: 0`.
- **IPv6 caveat:** local laptop is IPv6-only and cannot reach the Tansu VM
  (`34.24.194.122:9092`) or Cloud SQL directly. Run any synthetic injection or
  `delete_synthetic.py --prod` from Cloud Shell or on the VM, using the Cloud
  SQL Auth Proxy for catalog access.
- **Pre-existing drift left untouched:** the repo had unrelated uncommitted
  Terraform drift (probe schedule retiming, console annotations on several
  resources, etc.). These were intentionally not applied or committed as part of
  this work; the extractor and loader applies were `-target`-scoped to avoid
  them.
