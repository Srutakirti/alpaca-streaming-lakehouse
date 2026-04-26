resource "google_storage_bucket" "iceberg_warehouse" {
  name                        = "${var.project_id}-alpaca-iceberg-warehouse"
  location                    = var.region
  project                     = var.project_id
  uniform_bucket_level_access = true
  force_destroy               = var.allow_destroy

  lifecycle {
    prevent_destroy = false
  }
}

resource "google_storage_bucket" "tansu_storage" {
  name                        = "${var.project_id}-alpaca-tansu-storage"
  location                    = var.region
  project                     = var.project_id
  uniform_bucket_level_access = true
  force_destroy               = var.allow_destroy

  lifecycle {
    prevent_destroy = false
  }
}

resource "google_service_account" "tansu_sa" {
  account_id   = "tansu-broker"
  display_name = "Tansu Broker Service Account"
  project      = var.project_id
}

resource "google_storage_bucket_iam_member" "tansu_storage_admin" {
  bucket = google_storage_bucket.tansu_storage.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.tansu_sa.email}"
}

resource "google_storage_hmac_key" "tansu_hmac" {
  service_account_email = google_service_account.tansu_sa.email
  project               = var.project_id
}

resource "google_secret_manager_secret" "tansu_hmac_secret" {
  secret_id = "TANSU_HMAC_SECRET"
  project   = var.project_id

  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_version" "tansu_hmac_secret_val" {
  secret      = google_secret_manager_secret.tansu_hmac_secret.id
  secret_data = google_storage_hmac_key.tansu_hmac.secret
}
