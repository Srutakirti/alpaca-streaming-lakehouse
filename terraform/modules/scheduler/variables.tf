variable "project_id" {
  type = string
}

variable "region" {
  type    = string
  default = "us-east1"
}

variable "extractor_job_name" {
  type    = string
  default = "alpaca-extractor"
}

variable "start_schedule" {
  description = "Cron expression for starting the extractor (America/New_York)"
  type        = string
  default     = "30 9 * * 1-5"
}
