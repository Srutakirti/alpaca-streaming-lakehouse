output "kafka_broker_url" {
  description = "Tansu broker URL — set KAFKA_BROKER to this value for local testing"
  value       = module.tansu_broker.broker_url
}

output "iceberg_warehouse_bucket" {
  value = module.warehouse.iceberg_warehouse_bucket
}

output "tansu_storage_bucket" {
  value = module.warehouse.tansu_storage_bucket
}

output "cloudsql_instance_connection_name" {
  value = module.catalog.instance_connection_name
}

output "cloudsql_public_ip" {
  description = "Cloud SQL public IP — use for Phase 2 local loader with direct Postgres connection"
  value       = module.catalog.public_ip
}

output "catalog_uri_public" {
  description = "PyIceberg catalog URI for Phase 2 local loader (direct Postgres; requires authorized_networks)"
  value       = module.catalog.catalog_uri_public
  sensitive   = true
}

output "loader_service_url" {
  value = module.loader_service.service_url
}

output "frontend_service_url" {
  description = "Public URL for the dashboard SPA + API"
  value       = module.frontend_service.service_url
}

output "image_base" {
  value = module.artifact_registry.image_base
}

output "scheduler_start" {
  value = module.scheduler.start_job_name
}
