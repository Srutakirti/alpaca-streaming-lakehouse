"""Spark adapter for an Iceberg HadoopCatalog local warehouse."""

from __future__ import annotations

import os
import sys

from pyspark.sql import SparkSession

from .config import LocalSettings


class HadoopCatalog:
    """Owns a UTC-configured local Spark session and one Iceberg table."""

    catalog_name = "local"

    def __init__(self, settings: LocalSettings, spark: SparkSession | None = None) -> None:
        self.settings = settings
        self.spark = spark
        self._owns_spark = spark is None

    @property
    def identifier(self) -> str:
        return f"{self.catalog_name}.{self.settings.namespace}.{self.settings.table}"

    def start(self) -> SparkSession:
        if self.spark is not None:
            return self.spark
        self.settings.prepare()
        # Spark launches Python workers separately; make their interpreter match
        # the driver rather than inheriting a system Python of another version.
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
        builder = (
            SparkSession.builder.master("local[2]")
            .appName("gce-hadoop-catalog-local")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.pyspark.python", sys.executable)
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config(f"spark.sql.catalog.{self.catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
            .config(f"spark.sql.catalog.{self.catalog_name}.type", "hadoop")
            .config(f"spark.sql.catalog.{self.catalog_name}.warehouse", str(self.settings.warehouse_dir))
        )
        runtime_jar = os.environ.get("ICEBERG_SPARK_RUNTIME_JAR")
        if runtime_jar:
            builder = builder.config("spark.jars", runtime_jar)
        else:
            builder = builder.config(
                "spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2"
            )
        self.spark = builder.getOrCreate()
        return self.spark

    def ensure_table(self) -> None:
        spark = self.start()
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {self.catalog_name}.{self.settings.namespace}")
        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self.identifier} (
                event_id STRING NOT NULL,
                symbol STRING NOT NULL,
                event_time TIMESTAMP NOT NULL,
                open DOUBLE NOT NULL,
                high DOUBLE NOT NULL,
                low DOUBLE NOT NULL,
                close DOUBLE NOT NULL,
                volume BIGINT NOT NULL
            ) USING iceberg
            PARTITIONED BY (days(event_time), symbol)
            """
        )

    def count(self) -> int:
        self.ensure_table()
        return self.spark.table(self.identifier).count()  # type: ignore[union-attr]

    def close(self) -> None:
        if self.spark is not None and self._owns_spark:
            self.spark.stop()
        self.spark = None
