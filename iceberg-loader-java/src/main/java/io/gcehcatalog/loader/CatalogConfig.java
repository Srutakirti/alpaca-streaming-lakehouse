package io.gcehcatalog.loader;

import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;

/** Catalog-neutral loader configuration. The table contract does not change between catalog types. */
public record CatalogConfig(Type type, String name, String warehouse, String namespace, String table, String uri, String jdbcDriver, String jdbcUser, String jdbcPassword) {
  public enum Type { HADOOP, JDBC, REST }

  public static CatalogConfig fromEnvironment() {
    return from(Map.copyOf(System.getenv()), "ICEBERG_");
  }

  static CatalogConfig from(Map<String, String> environment) {
    return from(environment, "ICEBERG_");
  }

  static CatalogConfig from(Map<String, String> environment, String prefix) {
    Type type;
    try {
      type = Type.valueOf(environment.getOrDefault(prefix + "CATALOG_TYPE", "hadoop").toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException error) {
      throw new IllegalArgumentException("ICEBERG_CATALOG_TYPE must be hadoop, jdbc, or rest", error);
    }
    String warehouse = environment.getOrDefault(
        prefix + "WAREHOUSE", Path.of(".local-run", "warehouse").toAbsolutePath().toUri().toString());
    String uri = environment.getOrDefault(prefix + "CATALOG_URI", "");
    if (type != Type.HADOOP && uri.isBlank()) {
      throw new IllegalArgumentException("ICEBERG_CATALOG_URI is required for " + type.name().toLowerCase(Locale.ROOT));
    }
    return new CatalogConfig(
        type,
        environment.getOrDefault(prefix + "CATALOG_NAME", "alpaca"),
        warehouse,
        environment.getOrDefault(prefix + "NAMESPACE", "alpaca"),
        environment.getOrDefault(prefix + "TABLE", "bars_raw"),
        uri,
        environment.getOrDefault(prefix + "JDBC_DRIVER", ""),
        environment.getOrDefault(prefix + "CATALOG_USER", ""),
        environment.getOrDefault(prefix + "CATALOG_PASSWORD", ""));
  }
}
