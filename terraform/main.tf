module "warehouse" {
  source        = "./modules/warehouse"
  project_id    = var.project_id
  region        = var.region
  allow_destroy = var.allow_bucket_destroy
}

module "catalog" {
  source     = "./modules/catalog"
  project_id = var.project_id
  region     = var.region
  # Phase 2 local testing: use Cloud SQL Auth Proxy (no authorized_networks needed)
  authorized_networks = {}
  allow_destroy       = var.allow_db_destroy
}

module "artifact_registry" {
  source        = "./modules/artifact-registry"
  project_id    = var.project_id
  location      = var.artifact_registry_location
  repository_id = var.artifact_registry_repo
}

module "tansu_broker" {
  source     = "./modules/tansu-broker"
  project_id = var.project_id
  region     = var.region
  zone       = var.zone

  depends_on = [module.warehouse]
}

module "extractor_job" {
  source         = "./modules/extractor-job"
  project_id     = var.project_id
  region         = var.region
  image_base     = module.artifact_registry.image_base
  image_tag      = var.image_tag
  kafka_broker   = module.tansu_broker.broker_url
  kafka_topic    = var.kafka_topic
  alpaca_symbols = var.alpaca_symbols

  depends_on = [module.tansu_broker]
}

module "loader_service" {
  source                            = "./modules/loader-service"
  project_id                        = var.project_id
  region                            = var.region
  image_base                        = module.artifact_registry.image_base
  image_tag                         = var.image_tag
  kafka_broker                      = module.tansu_broker.broker_url
  kafka_topic                       = var.kafka_topic
  iceberg_catalog_uri               = module.catalog.catalog_uri_cloudsql
  iceberg_warehouse_bucket          = module.warehouse.iceberg_warehouse_bucket
  cloudsql_instance_connection_name = module.catalog.instance_connection_name
  batch_size                        = var.batch_size
  batch_interval                    = var.batch_interval

  depends_on = [module.catalog, module.tansu_broker]
}

module "frontend_service" {
  source                            = "./modules/frontend-service"
  project_id                        = var.project_id
  region                            = var.region
  image_base                        = module.artifact_registry.image_base
  image_tag                         = var.image_tag
  iceberg_catalog_uri               = module.catalog.catalog_uri_cloudsql
  iceberg_warehouse_bucket          = module.warehouse.iceberg_warehouse_bucket
  cloudsql_instance_connection_name = module.catalog.instance_connection_name

  depends_on = [module.catalog, module.warehouse]
}

module "scheduler" {
  source             = "./modules/scheduler"
  project_id         = var.project_id
  region             = var.region
  extractor_job_name = module.extractor_job.job_name
  start_schedule     = var.extractor_start_schedule

  depends_on = [module.extractor_job]
}

module "probe_job" {
  source                          = "./modules/probe-job"
  project_id                      = var.project_id
  region                          = var.region
  image_base                      = module.artifact_registry.image_base
  image_tag                       = var.image_tag
  alpaca_symbols                  = var.alpaca_symbols
  service_account_email           = module.extractor_job.service_account_email
  scheduler_service_account_email = module.scheduler.service_account_email

  depends_on = [module.extractor_job, module.scheduler]
}
