output "service_url" {
  value = google_cloud_run_v2_service.frontend.uri
}

output "service_account_email" {
  value = google_service_account.frontend.email
}
