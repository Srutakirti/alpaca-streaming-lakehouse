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

variable "alpaca_symbols" {
  type    = string
  default = "AAPL,TSLA"
}

variable "service_account_email" {
  description = "Reuses the alpaca-extractor SA (already has Secret Manager + Cloud Logging access)."
  type        = string
}

variable "scheduler_service_account_email" {
  description = "Reuses the alpaca-scheduler SA (already has roles/run.invoker)."
  type        = string
}

variable "probe_duration_seconds" {
  type    = number
  default = 120
}

variable "probe_paused" {
  description = "Pause all probe schedulers. The probe is retired (first-bar time is established and it collides with the extractor), but kept reversible."
  type        = bool
  default     = true
}

variable "schedules" {
  description = "Cron schedules (America/New_York) at which to fire the probe."
  type        = map(string)
  default = {
    probe_0530 = "30 5 * * 1-5"
    probe_0600 = "0 6 * * 1-5"
    probe_0630 = "30 6 * * 1-5"
    probe_0700 = "0 7 * * 1-5"
  }
}
