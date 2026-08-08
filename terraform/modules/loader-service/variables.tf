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

variable "kafka_broker" {
  type = string
}

variable "kafka_topic" {
  type    = string
  default = "alpaca-bars"
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

variable "batch_size" {
  type    = number
  default = 100
}

variable "batch_interval" {
  type    = number
  default = 300
}

variable "ingress" {
  description = "Cloud Run ingress policy. Default is internal-only because the loader is a background worker with only operational HTTP endpoints."
  type        = string
  default     = "INGRESS_TRAFFIC_INTERNAL_ONLY"
}
