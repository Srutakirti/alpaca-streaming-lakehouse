"""Read-only Spark configuration for local and future GCS HadoopCatalog exploration."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


ICEBERG_RUNTIME_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2"
GCS_CONNECTOR_DEFAULT = Path("/tmp/gcs-connector-hadoop3-2.2.30-shaded.jar")


@dataclass(frozen=True)
class SparkCatalogSettings:
    warehouse: str
    catalog_name: str = "alpaca"
    namespace: str = "alpaca"
    table: str = "bars_raw"
    gcs_enabled: bool = False
    gcs_connector_jar: Path = GCS_CONNECTOR_DEFAULT

    @property
    def table_identifier(self) -> str:
        return f"{self.catalog_name}.{self.namespace}.{self.table}"


def settings_from_environment() -> SparkCatalogSettings:
    raw_warehouse = os.environ.get("ICEBERG_WAREHOUSE")
    warehouse = raw_warehouse or (Path(".local-notebook") / "warehouse").resolve().as_uri()
    if "://" not in warehouse:
        warehouse = Path(warehouse).resolve().as_uri()
    gcs_enabled = os.environ.get("NOTEBOOK_ENABLE_GCS", "false").lower() == "true"
    if warehouse.startswith("gs://") and not gcs_enabled:
        raise ValueError("set NOTEBOOK_ENABLE_GCS=true before opening a gs:// warehouse")
    return SparkCatalogSettings(
        warehouse=warehouse,
        catalog_name=os.environ.get("NOTEBOOK_CATALOG_NAME", "alpaca"),
        namespace=os.environ.get("NOTEBOOK_NAMESPACE", "alpaca"),
        table=os.environ.get("NOTEBOOK_TABLE", "bars_raw"),
        gcs_enabled=gcs_enabled,
        gcs_connector_jar=Path(os.environ.get("GCS_CONNECTOR_JAR", GCS_CONNECTOR_DEFAULT)),
    )


def spark_configuration(settings: SparkCatalogSettings) -> dict[str, str]:
    """Return only read/query configuration; callers must not add write helpers here."""
    prefix = f"spark.sql.catalog.{settings.catalog_name}"
    configuration = {
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        f"{prefix}": "org.apache.iceberg.spark.SparkCatalog",
        f"{prefix}.type": "hadoop",
        f"{prefix}.warehouse": settings.warehouse,
        "spark.sql.session.timeZone": "UTC",
        "spark.jars.packages": ICEBERG_RUNTIME_PACKAGE,
    }
    if settings.warehouse.startswith("gs://"):
        if not settings.gcs_connector_jar.is_file():
            raise FileNotFoundError(f"GCS connector jar not found: {settings.gcs_connector_jar}")
        configuration.update(
            {
                "spark.jars": str(settings.gcs_connector_jar),
                "spark.hadoop.fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
                "spark.hadoop.fs.AbstractFileSystem.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
                "spark.hadoop.google.cloud.auth.type": "APPLICATION_DEFAULT",
            }
        )
    return configuration


def create_spark_session(settings: SparkCatalogSettings):
    """Create a local Spark exploration session without any write operation helpers."""
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName("iceberg-hadoop-catalog-explore").master("local[*]")
    for key, value in spark_configuration(settings).items():
        builder = builder.config(key, value)
    return builder.getOrCreate()
