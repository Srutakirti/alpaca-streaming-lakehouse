terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
    tls = {
      source  = "hashicorp/tls"
      version = "~> 4.0"
    }
  }
}

# Generate SSH key pair
resource "tls_private_key" "ssh" {
  algorithm = "RSA"
  rsa_bits  = 4096
}

# Create the VM instance
resource "google_compute_instance" "vm" {
  name         = var.name
  machine_type = var.machine_type
  zone         = var.zone
  project      = var.project_id

  boot_disk {
    initialize_params {
      # Ubuntu 24.04 LTS (Noble) - has glibc 2.39 required by Tansu
      image = "ubuntu-os-cloud/ubuntu-2404-lts-amd64"
      size  = 10
    }
  }

  network_interface {
    network = "default"
    access_config {
      # Ephemeral public IP
    }
  }

  metadata = {
    ssh-keys = "${var.ssh_user}:${tls_private_key.ssh.public_key_openssh}"
  }

  tags = ["tansu-server"]
}

# Firewall rule for SSH
resource "google_compute_firewall" "ssh" {
  name    = "${var.name}-allow-ssh"
  network = "default"
  project = var.project_id

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  source_ranges = ["0.0.0.0/0"]
  target_tags   = ["tansu-server"]
}

# Firewall rule for Kafka
resource "google_compute_firewall" "kafka" {
  name    = "${var.name}-allow-kafka"
  network = "default"
  project = var.project_id

  allow {
    protocol = "tcp"
    ports    = ["9092"]
  }

  source_ranges = ["0.0.0.0/0"]
  target_tags   = ["tansu-server"]
}

# Firewall rule for Prometheus metrics
resource "google_compute_firewall" "prometheus" {
  name    = "${var.name}-allow-prometheus"
  network = "default"
  project = var.project_id

  allow {
    protocol = "tcp"
    ports    = ["9100"]
  }

  source_ranges = ["0.0.0.0/0"]
  target_tags   = ["tansu-server"]
}
