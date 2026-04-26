variable "project_id" {
  type = string
}

variable "region" {
  type    = string
  default = "us-east1"
}

variable "authorized_networks" {
  description = "Map of name → CIDR for Cloud SQL authorized networks (for laptop access in Phase 2)"
  type        = map(string)
  default     = {}
}
