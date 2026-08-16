package io.gcehcatalog.loader;

import java.util.Map;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;

/** Registers existing table metadata in a target catalog without rewriting data files. */
public final class CatalogMigrator {
  private static final Namespace NAMESPACE = Namespace.of("alpaca");
  private static final TableIdentifier TABLE = TableIdentifier.of(NAMESPACE, "bars_raw");

  private CatalogMigrator() {}

  public static void main(String[] args) {
    if (args.length != 0) throw new IllegalArgumentException("CatalogMigrator reads ICEBERG_SOURCE_* and ICEBERG_* environment variables");
    migrate(CatalogConfig.from(Map.copyOf(System.getenv()), "ICEBERG_SOURCE_"), CatalogConfig.fromEnvironment());
  }

  static void migrate(CatalogConfig sourceConfig, CatalogConfig targetConfig) {
    Catalog source = CatalogFactory.open(sourceConfig);
    Catalog target = CatalogFactory.open(targetConfig);
    if (target.tableExists(TABLE)) throw new IllegalStateException("target catalog already contains " + TABLE);
    if (!(target instanceof SupportsNamespaces namespaces)) throw new IllegalArgumentException("target catalog does not support namespaces");
    if (!namespaces.namespaceExists(NAMESPACE)) namespaces.createNamespace(NAMESPACE);
    Table sourceTable = source.loadTable(TABLE);
    if (!(sourceTable instanceof HasTableOperations operations)) throw new IllegalStateException("source table does not expose Iceberg metadata location");
    String metadataLocation = operations.operations().current().metadataFileLocation();
    target.registerTable(TABLE, metadataLocation);
    System.out.println("catalog_migration=registered table=" + TABLE + " metadata=" + metadataLocation);
  }
}
