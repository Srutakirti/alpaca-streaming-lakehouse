output "iceberg_warehouse_bucket" {
  value = google_storage_bucket.iceberg_warehouse.name
}

output "tansu_storage_bucket" {
  value = google_storage_bucket.tansu_storage.name
}

output "tansu_hmac_access_key_id" {
  value     = google_storage_hmac_key.tansu_hmac.access_id
  sensitive = true
}

output "tansu_hmac_secret" {
  value     = google_storage_hmac_key.tansu_hmac.secret
  sensitive = true
}

output "tansu_sa_email" {
  value = google_service_account.tansu_sa.email
}
