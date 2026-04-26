resource "google_service_account" "scheduler" {
  account_id   = "alpaca-scheduler"
  display_name = "Alpaca Scheduler Service Account"
  project      = var.project_id
}

resource "google_project_iam_member" "scheduler_run_invoker" {
  project = var.project_id
  role    = "roles/run.invoker"
  member  = "serviceAccount:${google_service_account.scheduler.email}"
}

resource "google_cloud_scheduler_job" "extractor_start" {
  name             = "alpaca-extractor-start"
  description      = "Start the Alpaca extractor Cloud Run Job at market open"
  schedule         = var.start_schedule
  time_zone        = "America/New_York"
  project          = var.project_id
  region           = var.region
  attempt_deadline = "320s"

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${var.extractor_job_name}:run"

    oauth_token {
      service_account_email = google_service_account.scheduler.email
    }
  }
}

resource "google_cloud_scheduler_job" "extractor_stop" {
  name             = "alpaca-extractor-stop"
  description      = "Stop the Alpaca extractor at market close (deletes running executions)"
  schedule         = var.stop_schedule
  time_zone        = "America/New_York"
  project          = var.project_id
  region           = var.region
  attempt_deadline = "320s"

  http_target {
    http_method = "GET"
    # Lists and cancels running executions. Replace with a Cloud Function for production hardening.
    uri = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${var.extractor_job_name}/executions"

    oauth_token {
      service_account_email = google_service_account.scheduler.email
    }
  }
}
