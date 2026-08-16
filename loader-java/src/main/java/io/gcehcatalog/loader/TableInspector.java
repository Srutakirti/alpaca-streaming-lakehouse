package io.gcehcatalog.loader;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;

/** Small read-only inspection entry point used by local acceptance checks. */
public final class TableInspector {
  private TableInspector() {}

  public static void main(String[] args) throws Exception {
    if (args.length != 1) throw new IllegalArgumentException("usage: TableInspector <warehouse-uri>");
    try (HadoopCatalog catalog = new HadoopCatalog(new Configuration(), args[0])) {
      Table table = catalog.loadTable(TableIdentifier.of(Namespace.of("alpaca"), "bars_raw"));
      String count = table.currentSnapshot().summary().get("total-records");
      System.out.println("record_count=" + count);
    }
  }
}
