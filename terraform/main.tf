provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_compute_network" "pipeline" {
  name                    = "${var.resource_prefix}-network"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "pipeline" {
  name          = "${var.resource_prefix}-subnet"
  ip_cidr_range = "10.42.0.0/24"
  region        = var.region
  network       = google_compute_network.pipeline.id
}

resource "google_compute_firewall" "iap_ssh" {
  name      = "${var.resource_prefix}-iap-ssh"
  network   = google_compute_network.pipeline.name
  direction = "INGRESS"
  priority  = 1000

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  source_ranges = ["35.235.240.0/20"]
  target_tags   = ["${var.resource_prefix}-vm"]
}

resource "google_service_account" "vm" {
  account_id   = "${var.resource_prefix}-vm"
  display_name = "GCE HadoopCatalog pipeline VM"
}

resource "google_storage_bucket" "warehouse" {
  name                        = var.warehouse_bucket_name
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = false

  versioning {
    enabled = true
  }
}

resource "google_storage_bucket" "releases" {
  name                        = var.release_bucket_name
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = false

  versioning {
    enabled = true
  }
}

resource "google_storage_bucket_iam_member" "warehouse_writer" {
  bucket = google_storage_bucket.warehouse.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.vm.email}"
}

resource "google_storage_bucket_iam_member" "release_reader" {
  bucket = google_storage_bucket.releases.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.vm.email}"
}

resource "google_project_iam_member" "vm_logs_writer" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.vm.email}"
}

resource "google_project_iam_member" "vm_metrics_writer" {
  project = var.project_id
  role    = "roles/monitoring.metricWriter"
  member  = "serviceAccount:${google_service_account.vm.email}"
}

resource "google_compute_instance" "pipeline" {
  name         = "${var.resource_prefix}-vm"
  machine_type = var.machine_type
  zone         = var.zone
  tags         = ["${var.resource_prefix}-vm"]

  boot_disk {
    initialize_params {
      image = "projects/ubuntu-os-cloud/global/images/family/ubuntu-2404-lts-amd64"
      size  = 20
      type  = "pd-balanced"
    }
  }

  network_interface {
    subnetwork = google_compute_subnetwork.pipeline.id
    access_config {}
  }

  service_account {
    email  = google_service_account.vm.email
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
  }

  metadata_startup_script = templatefile("${path.module}/templates/startup.sh.tftpl", {
    warehouse_uri = "gs://${google_storage_bucket.warehouse.name}/warehouse"
  })
}
