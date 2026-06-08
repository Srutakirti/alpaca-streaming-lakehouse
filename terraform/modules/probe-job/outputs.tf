output "job_name" {
  value = google_cloud_run_v2_job.probe.name
}

output "scheduler_job_names" {
  value = [for j in google_cloud_scheduler_job.probe : j.name]
}
