package io.gcehcatalog.loader;

import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

/** Read-only inspection entry point that uses the same catalog selection as the loader. */
public final class TableInspector {
  private TableInspector() {}

  public static void main(String[] args) {
    if (args.length != 0) throw new IllegalArgumentException("TableInspector reads ICEBERG_CATALOG_* environment variables");
    CatalogConfig config = CatalogConfig.fromEnvironment();
    Catalog catalog = CatalogFactory.open(config);
    Table table = catalog.loadTable(TableIdentifier.of(Namespace.of(config.namespace()), config.table()));
    Snapshot snapshot = table.currentSnapshot();
    System.out.println("table=" + config.namespace() + "." + config.table());
    System.out.println("record_count=" + (snapshot == null ? 0 : snapshot.summary().get("total-records")));
  }
}
