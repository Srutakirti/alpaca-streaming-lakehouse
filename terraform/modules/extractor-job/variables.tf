variable "project_id" {
  type = string
}

variable "region" {
  type    = string
  default = "us-east1"
}

variable "image_base" {
  description = "Artifact Registry image base path (e.g. us-east1-docker.pkg.dev/project/repo)"
  type        = string
}

variable "image_tag" {
  type    = string
  default = "latest"
}

variable "kafka_broker" {
  description = "Kafka broker address (host:port)"
  type        = string
}

variable "kafka_topic" {
  type    = string
  default = "alpaca-bars"
}

variable "alpaca_symbols" {
  type    = string
  default = "AAPL,TSLA"
}
