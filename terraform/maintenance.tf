# Ad-hoc maintenance is a Serverless Spark batch, not a persistent cluster.
# This identity can rewrite the approved warehouse and use only its own
# short-lived staging bucket. It has no VM, Alpaca, dashboard, or billing access.
resource "google_project_service" "dataproc" {
  project            = var.project_id
  service            = "dataproc.googleapis.com"
  disable_on_destroy = false
}

resource "google_storage_bucket" "maintenance" {
  name                        = var.maintenance_bucket_name
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = false

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 7
    }
  }
}

resource "google_service_account" "iceberg_maintenance" {
  account_id   = "iceberg-maintenance"
  display_name = "Ad-hoc Iceberg compaction batch"
}

resource "google_project_iam_member" "iceberg_maintenance_dataproc_worker" {
  project = var.project_id
  role    = "roles/dataproc.worker"
  member  = "serviceAccount:${google_service_account.iceberg_maintenance.email}"
}

# Compaction must create replacement data and metadata objects and delete the
# superseded files after its atomic Iceberg commit. This scope is limited to
# the one warehouse bucket.
resource "google_storage_bucket_iam_member" "iceberg_maintenance_warehouse_admin" {
  bucket = google_storage_bucket.warehouse.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.iceberg_maintenance.email}"
}

resource "google_storage_bucket_iam_member" "iceberg_maintenance_staging_admin" {
  bucket = google_storage_bucket.maintenance.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.iceberg_maintenance.email}"
}
