variable "project_id" {
  type = string
}

variable "region" {
  type    = string
  default = "us-east1"
}

variable "image_base" {
  type = string
}

variable "image_tag" {
  type    = string
  default = "latest"
}

variable "iceberg_catalog_uri" {
  description = "PyIceberg catalog URI (Cloud SQL Auth Proxy unix socket path)"
  type        = string
  sensitive   = true
}

variable "iceberg_warehouse_bucket" {
  description = "GCS bucket name for Iceberg warehouse (without gs:// prefix)"
  type        = string
}

variable "cloudsql_instance_connection_name" {
  description = "Cloud SQL instance connection name for Cloud Run annotation"
  type        = string
}

variable "allow_unauthenticated" {
  description = "If true, grants roles/run.invoker to allUsers (public dashboard)."
  type        = bool
  default     = true
}
