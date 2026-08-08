resource "google_service_account" "loader" {
  account_id   = "alpaca-loader"
  display_name = "Alpaca Iceberg Loader Service Account"
  project      = var.project_id
}

resource "google_project_iam_member" "loader_cloudsql" {
  project = var.project_id
  role    = "roles/cloudsql.client"
  member  = "serviceAccount:${google_service_account.loader.email}"
}

resource "google_project_iam_member" "loader_log_writer" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.loader.email}"
}

resource "google_storage_bucket_iam_member" "loader_warehouse" {
  bucket = var.iceberg_warehouse_bucket
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.loader.email}"
}

resource "google_cloud_run_v2_service" "loader" {
  name     = "alpaca-loader"
  location = var.region
  project  = var.project_id
  ingress  = var.ingress

  template {
    service_account = google_service_account.loader.email

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
      image = "${var.image_base}/alpaca-loader:${var.image_tag}"

      env {
        name  = "GCP_PROJECT_ID"
        value = var.project_id
      }
      env {
        name  = "KAFKA_BROKER"
        value = var.kafka_broker
      }
      env {
        name  = "KAFKA_TOPIC"
        value = var.kafka_topic
      }
      env {
        name  = "KAFKA_GROUP_ID"
        value = "alpaca-iceberg-loader"
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
      env {
        name  = "BATCH_SIZE"
        value = tostring(var.batch_size)
      }
      env {
        name  = "BATCH_INTERVAL"
        value = tostring(var.batch_interval)
      }
      env {
        name  = "LOG_MODE"
        value = "both"
      }
      env {
        name  = "METRICS_INTERVAL"
        value = "10"
      }

      volume_mounts {
        name       = "cloudsql"
        mount_path = "/cloudsql"
      }

      ports {
        container_port = 8080
      }

      resources {
        limits = {
          cpu    = "1000m"
          memory = "512Mi"
        }
        # The loader is a background Kafka consumer: it calls
        # iceberg_table.append() inside its poll loop, OUTSIDE any HTTP request.
        # Cloud Run's default request-scoped CPU throttling starves that work
        # (appends took ~100-150s vs ~60ms with CPU). cpu_idle=false keeps the
        # CPU allocated for the always-on instance.
        cpu_idle          = false
        startup_cpu_boost = true
      }
    }
  }

  traffic {
    type    = "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST"
    percent = 100
  }
}
