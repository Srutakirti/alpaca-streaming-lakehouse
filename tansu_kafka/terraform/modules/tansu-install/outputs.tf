output "broker_url" {
  description = "Kafka broker URL for connecting to Tansu"
  value       = "${var.host}:9092"
}

output "prometheus_url" {
  description = "Prometheus metrics URL"
  value       = "http://${var.host}:9100"
}
