module "vm" {
  source     = "../../../tansu_kafka/terraform/modules/gcp-vm"
  project_id = var.project_id
  region     = var.region
  zone       = var.zone
  name       = "tansu-broker"
  ssh_user   = "tansu"
}

module "install" {
  source      = "../../../tansu_kafka/terraform/modules/tansu-install"
  host        = module.vm.public_ip
  private_key = module.vm.private_key
  ssh_user    = module.vm.ssh_user

  # memory:// — Tansu is a transport layer; Iceberg is the durable store.
  # GCS HMAC key creation is blocked by org policy iam.disableServiceAccountKeyCreation.
  storage_engine = "memory://"
}
