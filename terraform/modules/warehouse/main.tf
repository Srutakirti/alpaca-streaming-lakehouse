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
