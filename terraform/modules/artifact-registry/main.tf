data "google_artifact_registry_repository" "existing" {
  count       = 1
  location    = var.location
  repository_id = var.repository_id
  project     = var.project_id
}

resource "google_artifact_registry_repository" "repo" {
  # Only create if the data source lookup fails (repository does not exist yet).
  # In practice, the repo alpaca-datalake already exists per README.
  # Import it with: terraform import module.artifact_registry.google_artifact_registry_repository.repo ...
  count         = 0
  location      = var.location
  repository_id = var.repository_id
  format        = "DOCKER"
  project       = var.project_id
}
