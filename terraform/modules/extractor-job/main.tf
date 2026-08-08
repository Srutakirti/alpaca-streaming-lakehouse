resource "google_service_account" "extractor" {
  account_id   = "alpaca-extractor"
  display_name = "Alpaca Extractor Service Account"
  project      = var.project_id
}

resource "google_project_iam_member" "extractor_secret_accessor" {
  project = var.project_id
  role    = "roles/secretmanager.secretAccessor"
  member  = "serviceAccount:${google_service_account.extractor.email}"
}

resource "google_project_iam_member" "extractor_log_writer" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.extractor.email}"
}

resource "google_cloud_run_v2_job" "extractor" {
  name     = "alpaca-extractor"
  location = var.region
  project  = var.project_id

  template {
    template {
      service_account = google_service_account.extractor.email
      max_retries     = 3
      timeout         = "32400s" # 9h safety cap. The producer normally self-terminates after its idle window near market close.

      containers {
        # Rust producer (wsr). Reads Alpaca creds from env, not the GCP SDK —
        # so they are injected from Secret Manager below.
        image = "${var.image_base}/alpaca-extractor-rs:${var.image_tag}"

        env {
          name  = "KAFKA_BROKER"
          value = var.kafka_broker
        }
        env {
          name  = "KAFKA_TOPIC"
          value = var.kafka_topic
        }
        env {
          name  = "ALPACA_SYMBOLS"
          value = var.alpaca_symbols
        }
        env {
          name  = "RUST_LOG"
          value = "info"
        }
        env {
          name  = "METRICS_INTERVAL"
          value = "10"
        }
        env {
          name  = "MAX_RETRIES"
          value = "5"
        }
        env {
          name  = "TIMEOUT"
          value = "120"
        }

        # Secret Manager → env vars (the Rust binary has no GCP SDK). The
        # extractor SA already holds roles/secretmanager.secretAccessor.
        env {
          name = "ALPACA_KEY"
          value_source {
            secret_key_ref {
              secret  = "ALPACA_KEY"
              version = "latest"
            }
          }
        }
        env {
          name = "ALPACA_SECRET"
          value_source {
            secret_key_ref {
              secret  = "ALPACA_SECRET"
              version = "latest"
            }
          }
        }
      }
    }
  }
}
