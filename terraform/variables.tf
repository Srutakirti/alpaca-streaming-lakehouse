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

variable "dashboard_iceberg_metadata_prefix" {
  description = "Warehouse object prefix containing the one Iceberg table's metadata files for the public dashboard."
  type        = string
  default     = "warehouse/alpaca_candidate/bars_direct/metadata/"

  validation {
    condition     = can(regex("^warehouse/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/metadata/$", var.dashboard_iceberg_metadata_prefix))
    error_message = "dashboard_iceberg_metadata_prefix must be warehouse/NAMESPACE/TABLE/metadata/."
  }
}

variable "dashboard_billing_export_dataset_id" {
  description = "BigQuery dataset containing the standard Cloud Billing usage export."
  type        = string
  default     = "sink_billing"

  validation {
    condition     = can(regex("^[A-Za-z_][A-Za-z0-9_]*$", var.dashboard_billing_export_dataset_id))
    error_message = "dashboard_billing_export_dataset_id must be a valid BigQuery dataset ID."
  }
}

variable "dashboard_billing_export_table" {
  description = "Fully qualified standard Cloud Billing usage export table read by the private cost aggregate."
  type        = string
  default     = "project-66783f65-9c3e-4880-9a3.sink_billing.gcp_billing_export_v1_0157E4_698EC3_7F52B2"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{4,61}[a-z0-9]\\.[A-Za-z_][A-Za-z0-9_]*\\.[A-Za-z_][A-Za-z0-9_]*$", var.dashboard_billing_export_table))
    error_message = "dashboard_billing_export_table must be PROJECT.DATASET.TABLE."
  }
}

variable "dashboard_cost_dataset_id" {
  description = "Private aggregate dataset whose safe cost snapshot is read by the public dashboard exporter."
  type        = string
  default     = "dashboard_metrics"

  validation {
    condition     = can(regex("^[A-Za-z_][A-Za-z0-9_]*$", var.dashboard_cost_dataset_id))
    error_message = "dashboard_cost_dataset_id must be a valid BigQuery dataset ID."
  }
}

variable "dashboard_cost_schedule" {
  description = "BigQuery scheduled-query cadence for refreshing the public-safe cost snapshot."
  type        = string
  default     = "every 6 hours"
}
