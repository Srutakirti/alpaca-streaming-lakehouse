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

output "dashboard_metrics_service_account" {
  value = google_service_account.dashboard_metrics_reader.email
}

output "dashboard_workload_identity_provider" {
  value = google_iam_workload_identity_pool_provider.github_actions.name
}

output "dashboard_iceberg_metadata_uri" {
  value = "gs://${google_storage_bucket.warehouse.name}/${var.dashboard_iceberg_metadata_prefix}"
}

output "dashboard_cost_dataset" {
  value = google_bigquery_dataset.dashboard_costs.dataset_id
}

output "dashboard_cost_snapshot_table" {
  value = "${var.project_id}.${google_bigquery_dataset.dashboard_costs.dataset_id}.cost_snapshot"
}
