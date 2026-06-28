# Testing — Phase 2 (loader integration test: real Kafka → Iceberg)

This documents the loader **integration** test added in Phase 2 of `TESTING_PLAN.md`,
plus the `run_consumer` refactor that made the consume loop drivable from a test. Unlike
the Phase 1 unit tests (pure functions, no I/O), this test exercises the full
**Kafka → loader → Iceberg** path against a live broker.

Scope: one opt-in integration module (`load/tests/test_consume_integration.py`). It
builds on the shared harness in `phase-0-1-loader-tests.md` (the `tmp_iceberg` and
`make_frame` fixtures).

---

## How to run

Integration tests are **deselected by default** (they need a broker). Bring up Tansu,
then opt in with the `integration` marker:

```bash
# start a local broker (Phase 5 wraps this as `make up`)
docker run -d --name tansu -p 9092:9092 ghcr.io/tansu-io/tansu:0.6.0 \
  --storage-engine memory:// \
  --kafka-listener-url tcp://0.0.0.0:9092 \
  --kafka-advertised-listener-url tcp://localhost:9092

KAFKA_BROKER=localhost:9092 uv run pytest -m integration -v
```

Expected: `2 passed`. With Tansu down, the default `uv run pytest` simply skips them.

---

## Refactor — `run_consumer` extracted from `main()`

The consume loop lived inside `subscriber.main()`, which also opened the consumer, set
signal handlers, and closed everything in a `finally`. To drive the loop from a test
without `main()`'s process-level wiring, the loop body was extracted:

```python
def run_consumer(consumer, iceberg_table, metrics, logger, stop,
                 batch_size=BATCH_SIZE, batch_interval=BATCH_INTERVAL) -> None:
    """Poll the consumer and append batches to Iceberg until `stop` is set.
    At-least-once: offsets are committed only after a successful append.
    Does not close the consumer — the caller owns its lifecycle.
    """
```

Behavior is unchanged — `main()` now constructs the consumer/metrics/stop event and
calls `run_consumer`, then closes the consumer in its own `finally`. Two properties
matter for testing:

- **Caller owns the consumer.** `run_consumer` never closes it, so the test can inspect
  `consumer.committed(...)` *after* the loop stops — that's how at-least-once is verified.
- **`batch_size` / `batch_interval` are parameters.** Production uses the env defaults
  (100 / 300s); the test passes small values so a handful of frames flush immediately
  instead of waiting five minutes.

---

## The tests

### `test_kafka_to_iceberg_roundtrip`
The core path, end to end through a real broker:

1. Create a unique topic + consumer group (so parallel/repeat runs don't collide).
2. Produce 2 frames (`AAPL`×2, `TSLA`×3 bars = 5 records across 2 Kafka messages) with a
   plain `confluent_kafka.Producer`. `producer.list_topics(timeout=10)` forces the TCP
   handshake first — a Tansu quirk (it doesn't auto-create topics / lazily connect).
3. Run `run_consumer` in a daemon thread with `batch_size=5, batch_interval=1`, and poll
   `metrics.records_appended` until it reaches 5 (bounded by a 30s `_wait_until` helper),
   then set `stop`.
4. Assert: 5 rows in Iceberg with symbols `{AAPL, TSLA}`, **and** the committed offset on
   partition 0 equals the produced message count (2) — proving at-least-once
   (commit only after a successful append).

### `test_reload_from_catalog_sees_committed_rows`
Produces 4 `NVDA` bars, runs the consumer, then opens a **fresh** table via
`bootstrap_iceberg()` (as the frontend would) and asserts it sees all 4 rows. This proves
the consumer's committed Iceberg snapshot is visible to an independent reader — the
loader→frontend contract — not just to the in-memory table object the writer held.

---

## Why these patterns

- **Real broker + real Iceberg, hermetic per test** — unique topic/group names and the
  per-test `tmp_iceberg` warehouse keep runs isolated and repeatable despite shared infra.
- **Poll-until-condition, not sleep** — `_wait_until` waits on the actual metric
  (`records_appended`) with a timeout, avoiding flaky fixed sleeps.
- **Assert the durability contract, not just the data** — checking the committed offset
  (and the fresh-reload) verifies *at-least-once* and the catalog hand-off, which is the
  whole point of the loader.
- **Same test, local or cloud** — the broker comes from `$KAFKA_BROKER` (default
  `localhost:9092`); pointing at a cloud VM (`KAFKA_BROKER=<vm_ip>:9092`) runs the exact
  same assertions against the deployed broker. This is the Phase 6 deployment gate.

## Commit trail

| Commit subject | Phase |
|---|---|
| `test(load): add kafka-to-iceberg consume-loop integration test` | 2 |
