variable "host" {
  description = "IP address or hostname of the target VM"
  type        = string
}

variable "private_key" {
  description = "Private SSH key for connecting to the VM"
  type        = string
  sensitive   = true
}

variable "ssh_user" {
  description = "SSH username"
  type        = string
}

variable "tansu_version" {
  description = "Tansu version to install"
  type        = string
  default     = "v0.6.0"
}

variable "storage_engine" {
  description = "Tansu storage engine (memory://, postgres://, s3://)"
  type        = string
  default     = "memory://"
}

variable "kafka_listener_url" {
  description = "Kafka listener URL"
  type        = string
  default     = "tcp://0.0.0.0:9092"
}

variable "s3_access_key_id" {
  description = "HMAC access key ID for s3:// storage engine (leave empty for non-s3 engines)"
  type        = string
  default     = ""
  sensitive   = true
}

variable "s3_secret_access_key" {
  description = "HMAC secret access key for s3:// storage engine (leave empty for non-s3 engines)"
  type        = string
  default     = ""
  sensitive   = true
}

variable "s3_endpoint_url" {
  description = "S3-compatible endpoint URL (e.g. https://storage.googleapis.com for GCS)"
  type        = string
  default     = "https://storage.googleapis.com"
}
