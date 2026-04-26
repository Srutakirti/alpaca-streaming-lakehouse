output "iceberg_warehouse_bucket" {
  value = google_storage_bucket.iceberg_warehouse.name
}

output "tansu_storage_bucket" {
  value = google_storage_bucket.tansu_storage.name
}
