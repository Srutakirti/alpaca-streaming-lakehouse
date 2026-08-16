package io.gcehcatalog.loader;

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.rest.RESTCatalog;

/** Opens a catalog selected entirely by configuration; writer code only sees Catalog. */
public final class CatalogFactory {
  private CatalogFactory() {}

  public static Catalog open(CatalogConfig config) {
    return switch (config.type()) {
      case HADOOP -> new HadoopCatalog(new Configuration(), config.warehouse());
      case JDBC -> initializeJdbc(config);
      case REST -> initialize(new RESTCatalog(), config);
    };
  }

  private static Catalog initializeJdbc(CatalogConfig config) {
    if (!config.jdbcDriver().isBlank()) {
      try {
        Class.forName(config.jdbcDriver());
      } catch (ClassNotFoundException error) {
        throw new IllegalArgumentException("configured JDBC driver is not on the loader classpath", error);
      }
    }
    JdbcCatalog catalog = new JdbcCatalog();
    Map<String, String> properties = baseProperties(config);
    if (!config.jdbcUser().isBlank()) properties.put("jdbc.user", config.jdbcUser());
    if (!config.jdbcPassword().isBlank()) properties.put("jdbc.password", config.jdbcPassword());
    catalog.initialize(config.name(), properties);
    return catalog;
  }

  private static Catalog initialize(Catalog catalog, CatalogConfig config) {
    Map<String, String> properties = baseProperties(config);
    catalog.initialize(config.name(), properties);
    return catalog;
  }

  private static Map<String, String> baseProperties(CatalogConfig config) {
    Map<String, String> properties = new HashMap<>();
    properties.put("uri", config.uri());
    if (!config.warehouse().isBlank()) properties.put("warehouse", config.warehouse());
    return properties;
  }
}
