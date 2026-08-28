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
