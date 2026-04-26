module "vm" {
  source     = "../../../tansu_kafka/terraform/modules/gcp-vm"
  project_id = var.project_id
  region     = var.region
  zone       = var.zone
  name       = "tansu-broker"
  ssh_user   = "tansu"
}

module "install" {
  source     = "../../../tansu_kafka/terraform/modules/tansu-install"
  host       = module.vm.public_ip
  private_key = module.vm.private_key
  ssh_user   = module.vm.ssh_user

  storage_engine       = "s3://${var.tansu_storage_bucket}/"
  s3_access_key_id     = var.s3_access_key_id
  s3_secret_access_key = var.s3_secret_access_key
  s3_endpoint_url      = "https://storage.googleapis.com"
}
