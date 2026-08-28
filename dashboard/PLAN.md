# Public GitHub Pages operations dashboard

## Objective

Provide a public, read-only GitHub Pages dashboard for the GCE HadoopCatalog
pipeline. A scheduled GitHub Actions exporter obtains temporary, least-privilege
GCP access through Workload Identity Federation (WIF), reads selected Cloud
Logging entries, writes a sanitized `metrics.json`, and deploys it with the
static dashboard.

```text
GitHub Actions schedule
  -> GitHub OIDC token
  -> GCP WIF
  -> read-only Cloud Logging queries
  -> sanitized metrics.json
  -> public GitHub Pages dashboard
```

The dashboard never controls the VM, changes extractor settings, writes Iceberg
data, or exposes credentials or raw logs.

## Operating rule

Each checkpoint is implemented, validated, committed, and reported before the
next begins. Work proceeds automatically where no further authority is needed.
Creation of cloud IAM resources or public GitHub Pages publication requires the
relevant configured account and any necessary user-provided repository setting.

## Product decisions

- The dashboard is public. Published data must therefore be safe for anyone to
  read.
- The market-data session refreshes every 15 minutes; weekday off-hours refresh
  hourly; weekends refresh daily.
- All emitted timestamps and dashboard display values are UTC. State evaluation
  uses `America/New_York`.
- The current implementation is weekday-schedule-aware but not exchange-holiday
  aware.
- The dashboard uses static Vite + vanilla TypeScript, custom CSS, and small SVG
  charts. It has no browser-side GCP credential.
- The exporter is a Python program run through `uv`; it reads fixed queries and
  emits versioned, sanitized JSON.

## Dashboard states

| New York time | State | Expected behavior |
| --- | --- | --- |
| Before 09:30 ET | `pre_open` | No bars or commits are expected. |
| 09:30–16:00 ET | `market_open` | Bars and loader commits must remain fresh. |
| 16:00–17:00 ET | `settling` | Wait for final extractor metrics and final loader commit. |
| After 17:00 ET | `closed` | Preserve the completed session; missing bars are expected. |
| Saturday/Sunday | `weekend` | Preserve the prior session; no data is expected. |

Initial configurable thresholds are five and ten minutes for extractor bar
freshness, ten and fifteen minutes for loader commit freshness, and 17:00 ET
for a missing clean extractor shutdown.

## Published metrics

- Pipeline health, market state, generation time, and next expected open.
- Extractor connection state, last bar, received/sent counts, delivery failures,
  errors, final session metrics, and clean-shutdown reason.
- Loader last commit, commit age, received/inserted counts, and bounded recent
  commit history.
- Bounded warning/error summaries and prebuilt Cloud Logs Explorer links.

The generated JSON must exclude raw log messages, command lines, credentials,
VM hostnames, GCS paths, service-account identifiers, and raw symbol data.
VM host-memory charts are deferred because host-metric ingestion is intentionally
disabled on the e2-micro.

## Checkpoints

### D0: Dashboard design record

- Create this plan and the `dashboard/` directory.
- Record architecture, public-data boundary, time states, schedule, metrics, and
  validation approach.
- Commit the plan.

### D1: WIF infrastructure as code

- Add Terraform for a dedicated `github-pages-metrics-reader` service account,
  WIF pool/provider, repository/branch/workflow claim restrictions, and only
  required read IAM roles.
- Add variables and outputs without embedding repository secrets or IDs beyond
  public repository identity.
- Validate Terraform formatting and plan. Apply only with configured GCP access.
- Commit the checkpoint.

### D2: Exporter and metric contract

- Add a versioned `metrics.json` contract, sanitized Cloud Logging fixtures, and
  a `uv`-run Python exporter.
- Implement fixed read-only queries for the parsed extractor log and plain
  loader journal, state calculation, freshness, and bounded history.
- Unit-test normal market, settling, closed, weekend, stale, and error cases.
- Commit the checkpoint.

### D3: Static public dashboard

- Add the Vite + vanilla TypeScript dashboard, custom responsive UI, UTC labels,
  state-aware health presentation, charts, tables, and Logs Explorer links.
- Test a production build and fixture-driven rendering without cloud access.
- Commit the checkpoint.

### D4: Scheduled GitHub Actions and Pages deployment

- Add manual-dispatch and timezone-covering scheduled workflow triggers.
- Authenticate with GitHub OIDC and WIF; generate metrics; build the site; deploy
  the Pages artifact without committing generated metrics back to Git.
- Restrict privileged execution to the protected Pages/default branch, never
  pull-request code.
- Validate workflow syntax and commit the checkpoint.

### D5: Cloud and public acceptance

- Apply the reviewed Terraform WIF resources.
- Manually dispatch the workflow, verify it can read only the intended GCP data,
  and enable GitHub Pages if needed.
- Verify public output contains no prohibited fields, then validate market-open,
  market-close, and off-hours views.
- Commit evidence and report the final acceptance result.

## Open operational dependency

GitHub schedules execute from the repository's default branch. The dashboard
can be built and manually dispatched from this feature branch, but scheduled
operation requires the approved workflow to be present on the default branch
and that branch to be trusted by the WIF provider.
