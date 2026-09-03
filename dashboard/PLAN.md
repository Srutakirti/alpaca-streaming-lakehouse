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
- Bounded warning/error summaries with coded (not raw) failure causes.

The generated JSON must exclude raw log messages, command lines, credentials,
VM hostnames, GCS paths, service-account identifiers, and raw symbol data.
VM host-memory charts are deferred because host-metric ingestion is intentionally
disabled on the e2-micro.

## Checkpoints

### D0: Dashboard design record — complete (`4fafe0c`)

- Create this plan and the `dashboard/` directory.
- Record architecture, public-data boundary, time states, schedule, metrics, and
  validation approach.
- Commit the plan.

### D1: WIF infrastructure as code — complete; cloud apply deferred to D5

- Add Terraform for a dedicated `github-pages-metrics-reader` service account,
  WIF pool/provider, repository/branch/workflow claim restrictions, and only
  required read IAM roles.
- Add variables and outputs without embedding repository secrets or IDs beyond
  public repository identity.
- Validate Terraform formatting and plan. Apply only with configured GCP access.
- Commit the checkpoint.

### D2: Exporter and metric contract — complete

- Add a versioned `metrics.json` contract, sanitized Cloud Logging fixtures, and
  a `uv`-run Python exporter.
- Implement fixed read-only queries for the parsed extractor log and plain
  loader journal, state calculation, freshness, and bounded history.
- Unit-test normal market, settling, closed, weekend, stale, and error cases.
- Commit the checkpoint.

### D3: Static public dashboard — complete

- Add the Vite + vanilla TypeScript dashboard, custom responsive UI, UTC labels,
  state-aware health presentation, charts, tables, and coded alert summaries.
- Test a production build and fixture-driven rendering without cloud access.
- Commit the checkpoint.

### D4: Scheduled GitHub Actions and Pages deployment — complete; cloud acceptance deferred to D5

- Add manual-dispatch and timezone-covering scheduled workflow triggers.
- Authenticate with GitHub OIDC and WIF; generate metrics; build the site; deploy
  the Pages artifact without committing generated metrics back to Git.
- Restrict privileged execution to the protected Pages/default branch, never
  pull-request code.
- Validate workflow syntax and commit the checkpoint.

### D5: Cloud and public acceptance — complete

- Apply the reviewed Terraform WIF resources.
- Manually dispatch the workflow, verify it can read only the intended GCP data,
  and enable GitHub Pages if needed.
- Verify public output contains no prohibited fields, then validate market-open,
  market-close, and off-hours views.
- Commit evidence and report the final acceptance result.

Acceptance evidence, 2026-08-28:

- Terraform created the dedicated `gh-pages-metrics-reader` service account,
  its sole `roles/logging.viewer` grant, and the repository/branch/workflow
  restricted WIF pool, provider, and impersonation binding.
- Manual workflow run
  [`33192505473`](https://github.com/Srutakirti/alpaca-streaming-lakehouse/actions/runs/33192505473)
  succeeded: GitHub OIDC/WIF authentication, fixed Logging reads, metric
  generation, static build, artifact upload, and Pages deployment all passed.
- The public site is
  [srutakirti.github.io/alpaca-streaming-lakehouse](https://srutakirti.github.io/alpaca-streaming-lakehouse/).
  Its live `metrics.json` passed the schema and public-boundary check: no raw
  log fields, VM or GCS paths, project/service-account identifiers, symbols,
  credentials, or Cloud Logs links are published.
- The live snapshot accurately reported `market_open` and `unhealthy` for the
  current stale extractor/loader data. This is an operational state, not a
  dashboard deployment failure; clean-shutdown and bounded commit-history
  values remain visible.

## Open operational dependency

GitHub schedules execute from the repository's default branch. The dashboard
can be built and manually dispatched from this feature branch, but scheduled
operation requires the approved workflow to be present on the default branch
and that branch to be trusted by the WIF provider.

## Iceberg table-metadata extension

The dashboard will add a separate, public-safe table-metrics section sourced
from the current Iceberg metadata JSON. Logging remains the source of pipeline
health. Metadata freshness is informational initially, so a read failure does
not make a healthy producer/loader dashboard red.

### M1: Contract and parser

- Parse a supplied Iceberg metadata JSON into safe aggregate fields only.
- Validate current-snapshot selection, snapshot summary conversion, malformed
  metadata behavior, and no GCS-path/UUID/schema leakage.
- Commit the checkpoint.

Status: complete (`c5a95f6`).

### M2: Static table-metrics presentation

- Display current table totals, latest snapshot delta, and small-file
  maintenance indicators separately from loader batches.
- Commit after the production frontend build passes.

Status: complete (`8dea33d`).

### M3: Conditional GCS read identity

- Add a bucket IAM condition limited to the configured table `metadata/`
  prefix; add configuration and read the current version via `version-hint.text`.
- Review the Terraform plan before applying the new permission.

Status: complete (`8d416d5`). Terraform applied one conditional object-viewer
binding for `warehouse/alpaca_candidate/bars_direct/metadata/`; the GitHub
workflow receives the metadata URI as a repository variable.

### M4: Live acceptance

- Dispatch the workflow and compare safe published metrics against the current
  metadata JSON. Verify no metadata location, version, UUID, schema, or raw
  content is public.

Status: complete. Workflow run
[`33510687907`](https://github.com/Srutakirti/alpaca-streaming-lakehouse/actions/runs/33510687907)
succeeded on 2026-09-01. Its public metrics matched the current `v4531` table
metadata for the latest `append`, 4,623,785 total records, 4,530 data files,
899,223,658 total bytes, latest delta, and history counts. The published JSON
passed a boundary check for GCS paths, metadata version, UUID, manifests,
project ID, and VM identifier.

## Billing-cost extension

The public dashboard will publish exact project-scoped estimated usage cost in
USD. It is not an invoice total: Cloud Billing exports are asynchronous and may
revise recent usage with late credits or adjustments. All date buckets are UTC.
The raw billing export is never read by GitHub Actions or exposed publicly.

### C1: Private aggregate infrastructure

- Create a dedicated BigQuery dataset in the billing export's `us-east1`
  location and a dedicated scheduled-query service account.
- Run a six-hourly, overwrite-only scheduled query against the raw standard
  billing export. It writes one safe snapshot containing seven daily net costs,
  month-to-date net cost, the top three month-to-date services, currency, and
  export freshness.
- Give the dashboard identity `bigquery.dataViewer` only on the safe aggregate
  dataset. It receives neither raw billing access nor `bigquery.jobs.create`.
- Shift GitHub Actions cron minutes away from high-contention boundaries.

### C2: Exporter contract and UI

- Read the one aggregate snapshot with BigQuery table-data read access, not a
  new query job.
- Add an informational cost section: today-so-far (partial), seven-day total,
  MTD, export freshness, seven daily values, and three MTD services.
- Keep cost-read failures separate from pipeline health and publish no project,
  dataset, billing-account, SKU, label, resource, or raw billing details.

### C3: Live acceptance

- Review and apply the Terraform plan.
- Manually run the scheduled aggregate, compare it with an operator query
  against the raw export, then dispatch the dashboard workflow.
- Verify the public page and `metrics.json` show safe aggregate values only.
