data "google_artifact_registry_repository" "existing" {
  count         = 1
  location      = var.location
  repository_id = var.repository_id
  project       = var.project_id
}

resource "google_artifact_registry_repository" "repo" {
  # Only create if the data source lookup fails (repository does not exist yet).
  # In practice, the repo alpaca-datalake already exists per README, so count=0
  # and terraform never manages this resource. That means `terraform destroy`
  # cannot delete the Artifact Registry repository or any image inside it —
  # images pushed by scripts/build_and_push.sh persist across destroys.
  # If you ever flip count to 1, prevent_destroy keeps `terraform destroy`
  # from wiping the repository and its images.
  count         = 0
  location      = var.location
  repository_id = var.repository_id
  format        = "DOCKER"
  project       = var.project_id

  lifecycle {
    prevent_destroy = true
  }
}
