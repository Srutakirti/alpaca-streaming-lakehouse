output "start_job_name" {
  value = google_cloud_scheduler_job.extractor_start.name
}

output "stop_job_name" {
  value = google_cloud_scheduler_job.extractor_stop.name
}
