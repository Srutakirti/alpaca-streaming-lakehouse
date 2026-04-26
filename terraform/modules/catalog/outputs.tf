output "instance_connection_name" {
  value = google_sql_database_instance.catalog.connection_name
}

output "public_ip" {
  value = google_sql_database_instance.catalog.public_ip_address
}

output "db_user" {
  value = google_sql_user.iceberg.name
}

output "db_name" {
  value = google_sql_database.iceberg.name
}

output "db_password_secret_id" {
  value = google_secret_manager_secret.db_password.secret_id
}

output "db_password" {
  value     = random_password.db_password.result
  sensitive = true
}

output "catalog_uri_cloudsql" {
  description = "PyIceberg catalog URI for Cloud Run (via Cloud SQL Auth Proxy unix socket)"
  value       = "postgresql+psycopg2://iceberg:${random_password.db_password.result}@/iceberg?host=/cloudsql/${google_sql_database_instance.catalog.connection_name}"
  sensitive   = true
}

output "catalog_uri_public" {
  description = "PyIceberg catalog URI for direct access (Phase 2 local loader)"
  value       = "postgresql+psycopg2://iceberg:${random_password.db_password.result}@${google_sql_database_instance.catalog.public_ip_address}/iceberg"
  sensitive   = true
}
