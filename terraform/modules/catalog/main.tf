resource "random_password" "db_password" {
  length  = 24
  special = false
}

resource "google_sql_database_instance" "catalog" {
  name             = "alpaca-iceberg-catalog"
  region           = var.region
  project          = var.project_id
  database_version = "POSTGRES_15"

  settings {
    tier              = "db-f1-micro"
    availability_type = "ZONAL"
    disk_size         = 10
    disk_autoresize   = false

    backup_configuration {
      enabled = true
    }

    ip_configuration {
      ipv4_enabled = true

      dynamic "authorized_networks" {
        for_each = var.authorized_networks
        content {
          name  = authorized_networks.key
          value = authorized_networks.value
        }
      }
    }
  }

  deletion_protection = !var.allow_destroy
}

resource "google_sql_database" "iceberg" {
  name     = "iceberg"
  instance = google_sql_database_instance.catalog.name
  project  = var.project_id
}

resource "google_sql_user" "iceberg" {
  name     = "iceberg"
  instance = google_sql_database_instance.catalog.name
  password = random_password.db_password.result
  project  = var.project_id
}

resource "google_secret_manager_secret" "db_password" {
  secret_id = "ICEBERG_DB_PASSWORD"
  project   = var.project_id

  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_version" "db_password_val" {
  secret      = google_secret_manager_secret.db_password.id
  secret_data = random_password.db_password.result
}
