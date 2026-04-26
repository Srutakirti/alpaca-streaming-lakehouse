variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "us-east1"
}

variable "zone" {
  description = "GCP zone for the Tansu broker VM"
  type        = string
  default     = "us-east1-b"
}

variable "alpaca_symbols" {
  description = "Comma-separated list of Alpaca symbols for the extractor"
  type        = string
  default     = "AAPL,TSLA"
}

variable "image_tag" {
  description = "Docker image tag for extractor and loader (e.g. v0.1.0)"
  type        = string
  default     = "latest"
}

variable "artifact_registry_location" {
  description = "Artifact Registry repository location"
  type        = string
  default     = "us-east1"
}

variable "artifact_registry_repo" {
  description = "Artifact Registry repository name"
  type        = string
  default     = "alpaca-datalake"
}

variable "kafka_topic" {
  description = "Kafka topic name"
  type        = string
  default     = "alpaca-bars"
}

variable "batch_size" {
  description = "Number of records per Iceberg flush"
  type        = number
  default     = 100
}

variable "batch_interval" {
  description = "Max seconds between Iceberg flushes"
  type        = number
  default     = 300
}

variable "allow_bucket_destroy" {
  description = "Set to true to allow terraform destroy to delete GCS buckets (default: protected)"
  type        = bool
  default     = false
}

variable "extractor_start_schedule" {
  description = "Cloud Scheduler cron for starting the extractor (America/New_York)"
  type        = string
  default     = "0 8 * * 1-5"
}

variable "extractor_stop_schedule" {
  description = "Cloud Scheduler cron for stopping the extractor (America/New_York)"
  type        = string
  default     = "0 17 * * 1-5"
}
