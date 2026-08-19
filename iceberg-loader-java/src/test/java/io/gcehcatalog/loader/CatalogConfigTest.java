package io.gcehcatalog.loader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

class CatalogConfigTest {
  @Test void defaultsToLocalHadoopCatalog() {
    CatalogConfig config = CatalogConfig.from(Map.of());
    assertEquals(CatalogConfig.Type.HADOOP, config.type());
    assertEquals("alpaca", config.name());
    assertEquals("alpaca", config.namespace());
    assertEquals("bars_raw", config.table());
  }

  @Test void readsNamespaceAndTableSelection() {
    CatalogConfig config = CatalogConfig.from(Map.of(
        "ICEBERG_NAMESPACE", "alpaca_candidate", "ICEBERG_TABLE", "bars_direct"));
    assertEquals("alpaca_candidate", config.namespace());
    assertEquals("bars_direct", config.table());
  }

  @Test void acceptsJdbcAndRestCatalogsWithoutChangingTheWarehouseContract() {
    CatalogConfig jdbc = CatalogConfig.from(Map.of("ICEBERG_CATALOG_TYPE", "jdbc", "ICEBERG_CATALOG_URI", "jdbc:postgresql://catalog/iceberg", "ICEBERG_WAREHOUSE", "gs://warehouse"));
    CatalogConfig rest = CatalogConfig.from(Map.of("ICEBERG_CATALOG_TYPE", "rest", "ICEBERG_CATALOG_URI", "https://catalog.example/v1", "ICEBERG_WAREHOUSE", "gs://warehouse"));
    assertEquals(CatalogConfig.Type.JDBC, jdbc.type());
    assertEquals(CatalogConfig.Type.REST, rest.type());
  }

  @Test void requiresUriForNonHadoopCatalogs() {
    assertThrows(IllegalArgumentException.class, () -> CatalogConfig.from(Map.of("ICEBERG_CATALOG_TYPE", "rest")));
  }

  @Test void parsesAnIndependentSourceCatalogForMigration() {
    CatalogConfig source = CatalogConfig.from(Map.of("ICEBERG_SOURCE_CATALOG_TYPE", "hadoop", "ICEBERG_SOURCE_WAREHOUSE", "gs://old-warehouse"), "ICEBERG_SOURCE_");
    assertEquals(CatalogConfig.Type.HADOOP, source.type());
    assertEquals("gs://old-warehouse", source.warehouse());
  }

  @Test void configuresApplicationDefaultCredentialsOnlyForGcsHadoopWarehouses() {
    CatalogConfig gcs = CatalogConfig.from(Map.of("ICEBERG_WAREHOUSE", "gs://example/warehouse"));
    Configuration configuration = CatalogFactory.hadoopConfiguration(gcs);
    assertEquals("com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem", configuration.get("fs.gs.impl"));
    assertEquals("APPLICATION_DEFAULT", configuration.get("google.cloud.auth.type"));
  }
}
