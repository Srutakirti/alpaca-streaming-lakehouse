terraform {
  required_version = ">= 1.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
    tls = {
      source  = "hashicorp/tls"
      version = "~> 4.0"
    }
    null = {
      source  = "hashicorp/null"
      version = "~> 3.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# Create the GCP VM
module "vm" {
  source = "../modules/gcp-vm"

  project_id = var.project_id
  region     = var.region
  zone       = var.zone
  name       = "tansu-kafka"
}

# Install Tansu on the VM
module "tansu" {
  source = "../modules/tansu-install"

  host        = module.vm.public_ip
  private_key = module.vm.private_key
  ssh_user    = module.vm.ssh_user

  depends_on = [module.vm]
}
