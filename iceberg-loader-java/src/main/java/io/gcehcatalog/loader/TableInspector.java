package io.gcehcatalog.loader;

import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

/** Read-only inspection entry point that uses the same catalog selection as the loader. */
public final class TableInspector {
  private TableInspector() {}

  public static void main(String[] args) {
    if (args.length != 0) throw new IllegalArgumentException("TableInspector reads ICEBERG_CATALOG_* environment variables");
    Catalog catalog = CatalogFactory.open(CatalogConfig.fromEnvironment());
    Table table = catalog.loadTable(TableIdentifier.of(Namespace.of("alpaca"), "bars_raw"));
    System.out.println("record_count=" + table.currentSnapshot().summary().get("total-records"));
  }
}
