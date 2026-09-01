# Public operations dashboard

The public dashboard is a static GitHub Pages site. It displays a deliberately
small, safe summary of the GCE HadoopCatalog pipeline: state, UTC timestamps,
counts, recent loader commits, and coded alerts. It never includes raw logs,
credentials, hostnames, command lines, GCS paths, project IDs, service-account
identifiers, Cloud Logs links, or market records.

## Data path and permissions

```text
GitHub Actions schedule
  -> GitHub OIDC token
  -> GCP Workload Identity Federation
  -> gh-pages-metrics-reader service account (roles/logging.viewer)
  -> fixed Cloud Logging reads
  -> sanitized dashboard/public/metrics.json
  -> static GitHub Pages artifact
```

The workflow is [.github/workflows/dashboard-metrics.yml](../../.github/workflows/dashboard-metrics.yml).
It runs from the default branch only; it does not run pull-request code. Its
GCP identity is limited by the Terraform WIF provider to this repository, this
branch, and this exact workflow file.

## One-time setup

1. Apply the reviewed Terraform root using the existing deployment variables.
2. Copy its two dashboard outputs into GitHub **Repository variables** (not
   secrets), then create the third project-ID variable:

   | Repository variable | Value |
   | --- | --- |
   | `GCP_DASHBOARD_PROJECT_ID` | `project-66783f65-9c3e-4880-9a3` |
   | `GCP_DASHBOARD_WORKLOAD_IDENTITY_PROVIDER` | Terraform output `dashboard_workload_identity_provider` |
   | `GCP_DASHBOARD_METRICS_SERVICE_ACCOUNT` | Terraform output `dashboard_metrics_service_account` |
   | `GCP_DASHBOARD_ICEBERG_METADATA_URI` | Terraform output `dashboard_iceberg_metadata_uri` |

   These values identify public resources and contain no credential. The
   short-lived credential exists only inside the GitHub Actions job.

3. In GitHub repository **Settings → Pages**, set **Source** to **GitHub
   Actions**. The first successful deployment supplies the public site URL.
4. Use **Actions → Dashboard metrics → Run workflow** for the first validation.

## Schedule and operating behavior

GitHub Actions schedules are UTC. The workflow queries every 15 minutes over a
broad weekday window that covers 09:30–16:00 in both EST and EDT, hourly on
weekday off-hours, and once daily on weekends. The exporter—not cron—evaluates
the actual `America/New_York` state:

- `pre_open`: no bar activity expected;
- `market_open`: bar and loader freshness assessed;
- `settling`: awaits clean extractor shutdown and final load;
- `closed` / `weekend`: preserves the latest completed session without false
  stale-data alerts.

The initial design is not exchange-holiday aware.

The metadata URI is a GitHub repository variable, not public dashboard output.
The exporter reads only its `version-hint.text` and current `vNNN.metadata.json`
object. It emits aggregates such as counts and sizes; it never publishes the
URI, metadata version, manifest locations, schema, table UUID, or properties.

## Local UI validation

The dashboard fetches `metrics.json` at runtime. The committed
`metrics.example.json` is only a contract example and is not rendered by the
production page.

```bash
uv run python dashboard/export_metrics.py \
  --input tests/fixtures/dashboard/market_close.json \
  --now 2026-08-27T21:20:00Z \
  --output dashboard/public/metrics.json

cd dashboard
npm ci
npm run build
```

`dashboard/public/metrics.json` is ignored; remove it when the local check is
complete if desired. In normal operation GitHub Actions creates it freshly and
deploys it without committing it.

## Validation and troubleshooting

- A successful workflow build proves the GitHub OIDC token passed the WIF
  repository/branch/workflow restrictions and that the service account could
  read the intended log entries.
- If authentication fails immediately after Terraform apply, wait up to five
  minutes for IAM/WIF propagation, then manually dispatch once more.
- If the dashboard reports `no_recent_session` while markets are closed, that
  is informational—not a pipeline failure—until a session has completed.
- The dashboard has no GCP write capability. A failure cannot alter the VM,
  logs, Iceberg tables, or Cloud Logging configuration.
