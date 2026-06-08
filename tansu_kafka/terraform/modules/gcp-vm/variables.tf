variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "us-central1"
}

variable "zone" {
  description = "GCP zone"
  type        = string
  default     = "us-central1-a"
}

variable "machine_type" {
  description = "GCP machine type"
  type        = string
  default     = "e2-micro"
}

variable "name" {
  description = "VM instance name"
  type        = string
  default     = "tansu-vm"
}

variable "ssh_user" {
  description = "SSH username for the VM"
  type        = string
  default     = "tansu"
}
