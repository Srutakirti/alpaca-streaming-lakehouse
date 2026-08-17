variable "project_id" {
  description = "GCP project that owns the isolated Phase 2 resources."
  type        = string
}

variable "region" {
  description = "GCP region for the VM, subnet, and regional buckets."
  type        = string
  default     = "us-east1"
}

variable "zone" {
  description = "Zone for the single e2-micro VM."
  type        = string
  default     = "us-east1-b"
}

variable "resource_prefix" {
  description = "Short unique prefix for all resource names."
  type        = string
  default     = "gce-hadoop-catalog"
}

variable "warehouse_bucket_name" {
  description = "Globally unique GCS bucket name for Iceberg data and metadata."
  type        = string
}

variable "release_bucket_name" {
  description = "Globally unique GCS bucket name for immutable native release archives."
  type        = string
}

variable "machine_type" {
  description = "Keep the Phase 2 VM within the intended low-cost envelope."
  type        = string
  default     = "e2-micro"
}
