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

variable "github_dashboard_repository" {
  description = "GitHub owner/repository trusted to generate public dashboard data."
  type        = string
  default     = "Srutakirti/alpaca-streaming-lakehouse"

  validation {
    condition     = can(regex("^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$", var.github_dashboard_repository))
    error_message = "github_dashboard_repository must be in OWNER/REPOSITORY form."
  }
}

variable "github_dashboard_branch" {
  description = "Protected default branch that contains the scheduled dashboard workflow."
  type        = string
  default     = "architecture/gce-hadoop-catalog"

  validation {
    condition     = can(regex("^[A-Za-z0-9._/-]+$", var.github_dashboard_branch))
    error_message = "github_dashboard_branch contains unsupported characters."
  }
}

variable "github_dashboard_workflow_file" {
  description = "Workflow file trusted to read GCP logging for dashboard metrics."
  type        = string
  default     = "dashboard-metrics.yml"

  validation {
    condition     = can(regex("^[A-Za-z0-9._-]+\\.ya?ml$", var.github_dashboard_workflow_file))
    error_message = "github_dashboard_workflow_file must be a YAML filename."
  }
}
