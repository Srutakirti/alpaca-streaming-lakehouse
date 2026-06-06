output "start_job_name" {
  value = google_cloud_scheduler_job.extractor_start.name
}

output "service_account_email" {
  value = google_service_account.scheduler.email
}
