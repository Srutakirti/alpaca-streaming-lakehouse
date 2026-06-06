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
  description = "Number of records per Iceberg flush. Larger values amortize the per-flush GCS write cost (~25s) over more rows; recommended >=2000 under load."
  type        = number
  default     = 2000
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

variable "allow_db_destroy" {
  description = "Set to true to allow terraform destroy to delete the Cloud SQL catalog instance (default: protected via deletion_protection)"
  type        = bool
  default     = false
}

variable "extractor_start_schedule" {
  # 09:30 ET = market open. Probe data shows the first bar lands at 09:31 ET
  # every trading day; the extractor boots + subscribes in ~30-45s, so it is
  # ready before the first bar. (Was 08:00 ET, which idled 90 min and collided
  # with the 08:00 ET probe over Alpaca's single-connection limit.)
  description = "Cloud Scheduler cron for starting the extractor (America/New_York)"
  type        = string
  default     = "30 9 * * 1-5"
}

# No stop schedule: the producer self-terminates via its DATA_IDLE_TIMEOUT
# watchdog ~10 min after bars stop at market close. The old extractor-stop job
# was a no-op anyway (it only GET-listed executions, never cancelled them).
