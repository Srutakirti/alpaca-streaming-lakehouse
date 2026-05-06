resource "google_service_account" "frontend" {
  account_id   = "alpaca-frontend"
  display_name = "Alpaca Dashboard Frontend Service Account"
  project      = var.project_id
}

resource "google_project_iam_member" "frontend_cloudsql" {
  project = var.project_id
  role    = "roles/cloudsql.client"
  member  = "serviceAccount:${google_service_account.frontend.email}"
}

resource "google_project_iam_member" "frontend_log_writer" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.frontend.email}"
}

# Reads extractor + loader log entries to compute pipeline status.
resource "google_project_iam_member" "frontend_log_viewer" {
  project = var.project_id
  role    = "roles/logging.viewer"
  member  = "serviceAccount:${google_service_account.frontend.email}"
}

# Read-only access to the Iceberg warehouse bucket.
resource "google_storage_bucket_iam_member" "frontend_warehouse" {
  bucket = var.iceberg_warehouse_bucket
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.frontend.email}"
}

resource "google_cloud_run_v2_service" "frontend" {
  name     = "alpaca-frontend"
  location = var.region
  project  = var.project_id

  template {
    service_account = google_service_account.frontend.email

    scaling {
      min_instance_count = 1
      max_instance_count = 1
    }

    volumes {
      name = "cloudsql"
      cloud_sql_instance {
        instances = [var.cloudsql_instance_connection_name]
      }
    }

    containers {
      image = "${var.image_base}/alpaca-frontend:${var.image_tag}"

      env {
        name  = "GCP_PROJECT_ID"
        value = var.project_id
      }
      env {
        name  = "LOG_NAME_EXTRACTOR"
        value = "alpaca-stream"
      }
      env {
        name  = "ICEBERG_CATALOG_URI"
        value = var.iceberg_catalog_uri
      }
      env {
        name  = "ICEBERG_WAREHOUSE"
        value = "gs://${var.iceberg_warehouse_bucket}/"
      }
      env {
        name  = "ICEBERG_NAMESPACE"
        value = "alpaca"
      }
      env {
        name  = "ICEBERG_TABLE"
        value = "bars"
      }

      volume_mounts {
        name       = "cloudsql"
        mount_path = "/cloudsql"
      }

      ports {
        container_port = 8080
      }
    }
  }

  traffic {
    type    = "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST"
    percent = 100
  }
}

resource "google_cloud_run_v2_service_iam_member" "public" {
  count    = var.allow_unauthenticated ? 1 : 0
  project  = var.project_id
  location = google_cloud_run_v2_service.frontend.location
  name     = google_cloud_run_v2_service.frontend.name
  role     = "roles/run.invoker"
  member   = "allUsers"
}
