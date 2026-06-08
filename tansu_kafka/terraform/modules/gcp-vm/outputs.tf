output "public_ip" {
  description = "Public IP address of the VM"
  value       = google_compute_instance.vm.network_interface[0].access_config[0].nat_ip
}

output "private_key" {
  description = "Private SSH key for connecting to the VM"
  value       = tls_private_key.ssh.private_key_pem
  sensitive   = true
}

output "ssh_user" {
  description = "SSH username"
  value       = var.ssh_user
}

output "instance_name" {
  description = "Name of the VM instance"
  value       = google_compute_instance.vm.name
}
