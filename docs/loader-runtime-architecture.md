# Loader Runtime Architecture

This note captures the current loader design and the tradeoffs we discussed around Cloud Run scale-to-zero, Kafka polling, REST endpoints, and future ergonomic options.

## Current Shape

`alpaca-loader` is deployed as a Cloud Run Service.

It currently has two responsibilities:

| Responsibility | What it does |
| --- | --- |
| Background worker | Polls Kafka/Tansu, batches bars, appends to Iceberg, commits Kafka offsets. |
| HTTP service | Exposes health/status-style HTTP endpoints through Cloud Run. |

This dual role is why the loader is currently a Cloud Run Service rather than only a Cloud Run Job.

## Kafka Polling And Scale-To-Zero

The loader process has an infinite Kafka polling loop. That does not, by itself, prevent Cloud Run from stopping the instance when the service is allowed to scale to zero.

For Cloud Run Services, the important control is:

```text
min-instances
```

Behavior:

| Setting | Behavior |
| --- | --- |
| `min-instances=1` | Cloud Run keeps one loader instance warm. The loader keeps polling Kafka even if there are no messages. |
| `min-instances=0` | Cloud Run is allowed to terminate the idle service instance. The infinite loop does not guarantee the process stays alive. |

Kafka messages do not wake a Cloud Run Service automatically. Scale-from-zero for a service is normally triggered by HTTP traffic, not by Kafka topic data.

That means the loader must be started before the extractor starts producing:

```text
09:05 ET -> alpaca-infra-start -> loader min-instances=1
09:30 ET -> alpaca-extractor-start -> extractor produces to Kafka
17:20 ET -> alpaca-infra-stop  -> loader min-instances=0
```

## Cost Implication

The loader runs background work outside HTTP request handling. For that to work reliably on Cloud Run, CPU throttling is disabled:

```text
cpu_idle=false
```

That is correct for a polling consumer, but it makes cost control important.

| State | Cost model |
| --- | --- |
| Market hours | `min-instances=1`, loader stays alive, CPU can be allocated while polling. |
| After market | `min-instances=0`, loader can scale to zero and stop polling. |

So the daily `alpaca-infra-stop` job is required for cost control. Market close alone does not stop the loader.

## Current Operational Model

The current model is schedule-controlled:

```text
Cloud Scheduler
    -> alpaca-infra-start Cloud Run Job
        -> start Tansu VM
        -> start Cloud SQL
        -> set alpaca-loader min-instances=1

Cloud Scheduler
    -> alpaca-extractor-start
        -> run Alpaca extractor

Cloud Scheduler
    -> alpaca-infra-stop Cloud Run Job
        -> set alpaca-loader min-instances=0
        -> stop Cloud SQL
        -> stop Tansu VM
```

This is acceptable for the current stage because the market session has predictable hours.

## Ergonomic Alternatives

### 1. Keep Current Design

Keep the loader as a Cloud Run Service and use schedules to control `min-instances`.

Pros:

- Minimal change.
- Keeps current HTTP endpoints.
- Easy to inspect through Cloud Run.
- Works with the current frontend and monitoring assumptions.

Cons:

- A background worker in a request-oriented service is conceptually awkward.
- Cost control depends on schedule correctness.
- Kafka messages cannot wake the loader from zero.

This is the recommended short-term option.

### 2. Move Loader Worker To A Cloud Run Job

Run the loader as a long-running Cloud Run Job during market hours.

Pros:

- More natural for a bounded market-session worker.
- No `min-instances` tuning.
- Job lifecycle matches extractor lifecycle better.

Cons:

- Current REST endpoints would need to move somewhere else or be removed.
- Need explicit shutdown and timeout behavior.
- Monitoring needs to read logs/status from the job or a separate status store.

### 3. Move Loader Worker To The Tansu VM

Run Tansu and loader together on the same VM under `systemd`.

Pros:

- Most ergonomic cost control for the current setup: VM on means broker and loader are both on.
- One start/stop target.
- No Cloud Run background-service mismatch.

Cons:

- Less managed than Cloud Run.
- Need VM deployment/update process for loader changes.
- Loader HTTP endpoints would either be internal-only or require firewall/auth/proxy design.

### 4. Split Worker And API

Separate the worker from the HTTP API:

```text
loader worker
    -> writes metrics/status to Cloud Logging or a small status table

frontend/API service
    -> reads Cloud Logging/status table
    -> exposes monitoring endpoints
```

Pros:

- Cleanest long-term architecture.
- Loader can run as a Job, VM process, or worker pool.
- HTTP service remains request-oriented.

Cons:

- Requires moving current loader endpoints or status logic.
- Requires a clear status contract.

This is the best long-term direction if the system grows.

### 5. Cloud Run Worker Pool

Cloud Run Worker Pools are designed for non-HTTP background workloads.

Pros:

- Better semantic fit than Cloud Run Service for a worker.
- Managed Cloud Run runtime.

Cons:

- Does not automatically scale from Kafka messages by itself.
- Still needs explicit instance control or a Kafka-lag autoscaler.
- More moving parts than needed for a simple market-hours schedule.

## Recommendation

For now:

```text
Keep alpaca-loader as a Cloud Run Service.
Keep schedule-controlled min-instances.
Keep alpaca-infra-stop as the cost-control boundary.
```

Next architectural improvement:

```text
Split the loader worker from the HTTP/status API.
```

That lets the worker move to a more natural runtime later without losing monitoring ergonomics.
