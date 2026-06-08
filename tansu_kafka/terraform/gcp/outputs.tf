output "broker_url" {
  description = "Kafka broker URL for connecting to Tansu"
  value       = module.tansu.broker_url
}

output "vm_ip" {
  description = "Public IP address of the VM"
  value       = module.vm.public_ip
}

output "ssh_private_key" {
  description = "Private SSH key for connecting to the VM"
  value       = module.vm.private_key
  sensitive   = true
}

output "ssh_command" {
  description = "SSH command to connect to the VM"
  value       = "ssh -i ~/.ssh/tansu_vm.pem ${module.vm.ssh_user}@${module.vm.public_ip}"
}

output "prometheus_url" {
  description = "Prometheus metrics URL"
  value       = module.tansu.prometheus_url
}
