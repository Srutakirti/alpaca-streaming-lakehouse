resource "google_cloud_run_v2_job" "probe" {
  name     = "alpaca-probe"
  location = var.region
  project  = var.project_id

  template {
    template {
      service_account = var.service_account_email
      max_retries     = 0
      timeout         = "${var.probe_duration_seconds + 60}s"

      containers {
        image   = "${var.image_base}/alpaca-extractor:${var.image_tag}"
        command = ["uv", "run", "--package", "extract", "python", "extract/probe.py"]

        env {
          name  = "GCP_PROJECT_ID"
          value = var.project_id
        }
        env {
          name  = "ALPACA_SYMBOLS"
          value = var.alpaca_symbols
        }
        env {
          name  = "LOG_MODE"
          value = "both"
        }
        env {
          name  = "PROBE_DURATION_SECONDS"
          value = tostring(var.probe_duration_seconds)
        }
      }
    }
  }
}

resource "google_cloud_scheduler_job" "probe" {
  for_each = var.schedules

  name        = replace(each.key, "_", "-")
  description = "Alpaca market-open probe (${each.value} ET)"
  schedule    = each.value
  time_zone   = "America/New_York"
  project     = var.project_id
  region      = var.region
  # Paused: the probe has established that the first bar lands at 09:31 ET every
  # trading day, so it no longer has a question to answer — and it collides with
  # the market-hours extractor over Alpaca's single-connection limit. Reversible:
  # set paused=false (or var.probe_paused) to bring it back.
  paused           = var.probe_paused
  attempt_deadline = "320s"

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${google_cloud_run_v2_job.probe.name}:run"

    oauth_token {
      service_account_email = var.scheduler_service_account_email
    }
  }
}
