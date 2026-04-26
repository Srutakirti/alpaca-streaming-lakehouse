output "registry_hostname" {
  value = "${var.location}-docker.pkg.dev"
}

output "image_base" {
  value = "${var.location}-docker.pkg.dev/${var.project_id}/${var.repository_id}"
}
