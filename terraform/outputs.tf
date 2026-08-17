output "vm_name" {
  value = google_compute_instance.pipeline.name
}

output "vm_zone" {
  value = google_compute_instance.pipeline.zone
}

output "warehouse_uri" {
  value = "gs://${google_storage_bucket.warehouse.name}/warehouse"
}

output "release_bucket" {
  value = google_storage_bucket.releases.name
}

output "vm_service_account" {
  value = google_service_account.vm.email
}
