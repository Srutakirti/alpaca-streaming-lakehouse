# GitHub Actions uses this identity only to generate the public dashboard data.
# It has no VM, storage, Secret Manager, or write permissions.
resource "google_service_account" "dashboard_metrics_reader" {
  account_id   = "gh-pages-metrics-reader"
  display_name = "GitHub Pages dashboard metrics reader"
}

resource "google_project_iam_member" "dashboard_metrics_logging_viewer" {
  project = var.project_id
  role    = "roles/logging.viewer"
  member  = "serviceAccount:${google_service_account.dashboard_metrics_reader.email}"
}

# The cost aggregate runs separately from GitHub Actions. It can read the raw
# billing export and write one public-safe snapshot, while the dashboard
# identity below can read only that safe dataset.
data "google_project" "current" {
  project_id = var.project_id
}

resource "google_project_service" "bigquery_data_transfer" {
  project            = var.project_id
  service            = "bigquerydatatransfer.googleapis.com"
  disable_on_destroy = false
}

resource "google_bigquery_dataset" "dashboard_costs" {
  project       = var.project_id
  dataset_id    = var.dashboard_cost_dataset_id
  friendly_name = "Public dashboard cost aggregates"
  description   = "Contains only public-safe Cloud Billing cost aggregates for the GitHub Pages dashboard."
  location      = "us-east1"

  delete_contents_on_destroy = false
}

resource "google_service_account" "dashboard_cost_aggregator" {
  account_id   = "gh-pages-cost-aggregator"
  display_name = "GitHub Pages dashboard cost aggregator"
}

resource "google_project_iam_member" "dashboard_cost_aggregator_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.dashboard_cost_aggregator.email}"
}

resource "google_bigquery_dataset_iam_member" "dashboard_cost_aggregator_billing_reader" {
  project    = var.project_id
  dataset_id = var.dashboard_billing_export_dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.dashboard_cost_aggregator.email}"
}

resource "google_bigquery_dataset_iam_member" "dashboard_cost_aggregator_writer" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.dashboard_costs.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.dashboard_cost_aggregator.email}"
}

# BigQuery Data Transfer Service uses this permission to mint temporary tokens
# for the dedicated aggregate-writer service account at scheduled-query time.
resource "google_service_account_iam_member" "dashboard_cost_aggregator_transfer_token_creator" {
  service_account_id = google_service_account.dashboard_cost_aggregator.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = "serviceAccount:service-${data.google_project.current.number}@gcp-sa-bigquerydatatransfer.iam.gserviceaccount.com"
}

resource "google_bigquery_data_transfer_config" "dashboard_cost_snapshot" {
  project                = var.project_id
  location               = google_bigquery_dataset.dashboard_costs.location
  display_name           = "Public dashboard cost snapshot"
  data_source_id         = "scheduled_query"
  destination_dataset_id = google_bigquery_dataset.dashboard_costs.dataset_id
  schedule               = var.dashboard_cost_schedule
  service_account_name   = google_service_account.dashboard_cost_aggregator.email
  schedule_options {
    disable_auto_scheduling = false
  }

  params = {
    destination_table_name_template = "cost_snapshot"
    write_disposition               = "WRITE_TRUNCATE"
    query                           = <<-SQL
      -- This query intentionally exposes only project-level, public-safe cost
      -- aggregates. It does not copy raw billing line items into the target.
      WITH line_items AS (
        SELECT
          DATE(usage_start_time, "UTC") AS usage_day_utc,
          service.description AS service_name,
          currency,
          CAST(cost AS NUMERIC) + IFNULL((
            SELECT SUM(CAST(credit.amount AS NUMERIC))
            FROM UNNEST(credits) AS credit
          ), 0) AS net_cost,
          export_time
        FROM `${var.dashboard_billing_export_table}`
        WHERE project.id = "${var.project_id}"
          AND DATE(usage_start_time, "UTC") >= LEAST(
            DATE_SUB(CURRENT_DATE("UTC"), INTERVAL 6 DAY),
            DATE_TRUNC(CURRENT_DATE("UTC"), MONTH)
          )
      ),
      daily AS (
        SELECT usage_day_utc, currency, SUM(net_cost) AS net_cost
        FROM line_items
        WHERE usage_day_utc >= DATE_SUB(CURRENT_DATE("UTC"), INTERVAL 6 DAY)
        GROUP BY usage_day_utc, currency
      ),
      monthly_services AS (
        SELECT service_name, currency, SUM(net_cost) AS net_cost
        FROM line_items
        WHERE usage_day_utc >= DATE_TRUNC(CURRENT_DATE("UTC"), MONTH)
        GROUP BY service_name, currency
      )
      SELECT
        CURRENT_TIMESTAMP() AS aggregated_at_utc,
        MAX(export_time) AS source_exported_at_utc,
        (SELECT ANY_VALUE(currency) FROM line_items) AS currency,
        (SELECT COALESCE(SUM(net_cost), 0) FROM daily) AS seven_day_net_cost,
        (SELECT COALESCE(SUM(net_cost), 0) FROM monthly_services) AS month_to_date_net_cost,
        ARRAY(
          SELECT AS STRUCT CAST(usage_day_utc AS STRING) AS day_utc, net_cost
          FROM daily
          ORDER BY usage_day_utc
        ) AS daily_costs,
        ARRAY(
          SELECT AS STRUCT service_name, net_cost
          FROM monthly_services
          ORDER BY net_cost DESC, service_name
          LIMIT 3
        ) AS top_services
      FROM line_items
    SQL
  }

  depends_on = [
    google_project_service.bigquery_data_transfer,
    google_bigquery_dataset_iam_member.dashboard_cost_aggregator_billing_reader,
    google_bigquery_dataset_iam_member.dashboard_cost_aggregator_writer,
    google_project_iam_member.dashboard_cost_aggregator_job_user,
    google_service_account_iam_member.dashboard_cost_aggregator_transfer_token_creator,
  ]
}

# This account receives reader access only to the safe aggregate dataset, not
# to the raw billing export and not permission to execute arbitrary BQ queries.
resource "google_bigquery_dataset_iam_member" "dashboard_metrics_cost_reader" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.dashboard_costs.dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.dashboard_metrics_reader.email}"
}

# The public-dashboard exporter can read only this table's Iceberg metadata
# directory. It cannot read Parquet data files, manifests elsewhere, or write.
resource "google_storage_bucket_iam_member" "dashboard_metadata_reader" {
  bucket = google_storage_bucket.warehouse.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.dashboard_metrics_reader.email}"

  condition {
    title       = "dashboard_iceberg_metadata_only"
    description = "Read only the configured public-dashboard Iceberg metadata objects"
    expression  = "resource.name.startsWith('projects/_/buckets/${google_storage_bucket.warehouse.name}/objects/${var.dashboard_iceberg_metadata_prefix}')"
  }
}

# GitHub's OIDC issuer is trusted only for the dashboard workflow on the
# configured repository branch. The action exchanges that proof for temporary
# credentials; no service-account key is created or stored in GitHub.
resource "google_iam_workload_identity_pool" "github_actions" {
  workload_identity_pool_id = "github-actions"
  display_name              = "GitHub Actions"
  description               = "GitHub Actions identities for the public dashboard"
}

resource "google_iam_workload_identity_pool_provider" "github_actions" {
  workload_identity_pool_id          = google_iam_workload_identity_pool.github_actions.workload_identity_pool_id
  workload_identity_pool_provider_id = "dashboard"
  display_name                       = "GitHub dashboard workflow"
  description                        = "Trusts only the configured dashboard workflow and branch"

  attribute_mapping = {
    "google.subject"         = "assertion.sub"
    "attribute.repository"   = "assertion.repository"
    "attribute.ref"          = "assertion.ref"
    "attribute.workflow_ref" = "assertion.workflow_ref"
  }

  attribute_condition = "assertion.repository == '${var.github_dashboard_repository}' && assertion.ref == 'refs/heads/${var.github_dashboard_branch}' && assertion.workflow_ref == '${var.github_dashboard_repository}/.github/workflows/${var.github_dashboard_workflow_file}@refs/heads/${var.github_dashboard_branch}'"

  oidc {
    issuer_uri = "https://token.actions.githubusercontent.com"
  }
}

# The principal set grants impersonation only to GitHub jobs that have the
# mapped repository attribute. The provider condition above further limits the
# exchange to the configured branch and workflow file.
resource "google_service_account_iam_member" "dashboard_metrics_workload_identity_user" {
  service_account_id = google_service_account.dashboard_metrics_reader.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.github_actions.name}/attribute.repository/${var.github_dashboard_repository}"
}
