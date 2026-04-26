variable "project_id" {
  type = string
}

variable "region" {
  type    = string
  default = "us-east1"
}

variable "allow_destroy" {
  description = "Allow terraform destroy to delete GCS buckets"
  type        = bool
  default     = false
}
