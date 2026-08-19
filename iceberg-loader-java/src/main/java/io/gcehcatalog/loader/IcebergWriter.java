package io.gcehcatalog.loader;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;

/** Direct Iceberg Java writer: no Spark runtime is created and the catalog is configurable. */
public final class IcebergWriter implements AutoCloseable {
  static final Schema SCHEMA = new Schema(
      Types.NestedField.required(1, "payload_hash", Types.StringType.get()),
      Types.NestedField.required(2, "T", Types.StringType.get()),
      Types.NestedField.required(3, "S", Types.StringType.get()),
      Types.NestedField.required(4, "o", Types.DoubleType.get()),
      Types.NestedField.required(5, "h", Types.DoubleType.get()),
      Types.NestedField.required(6, "l", Types.DoubleType.get()),
      Types.NestedField.required(7, "c", Types.DoubleType.get()),
      Types.NestedField.required(8, "v", Types.LongType.get()),
      Types.NestedField.required(9, "t", Types.StringType.get()),
      Types.NestedField.required(10, "n", Types.LongType.get()),
      Types.NestedField.required(11, "vw", Types.DoubleType.get()),
      Types.NestedField.required(12, "ingested_at", Types.StringType.get()));

  private final Catalog catalog;
  private final SupportsNamespaces namespaces;
  private final Table table;

  public IcebergWriter(CatalogConfig config) {
    this.catalog = CatalogFactory.open(config);
    if (!(catalog instanceof SupportsNamespaces supportsNamespaces)) {
      throw new IllegalArgumentException("configured catalog does not support namespaces");
    }
    this.namespaces = supportsNamespaces;
    Namespace namespace = Namespace.of(config.namespace());
    TableIdentifier tableIdentifier = TableIdentifier.of(namespace, config.table());
    if (!namespaces.namespaceExists(namespace)) namespaces.createNamespace(namespace);
    this.table = catalog.tableExists(tableIdentifier)
        ? catalog.loadTable(tableIdentifier)
        : catalog.createTable(tableIdentifier, SCHEMA, PartitionSpec.unpartitioned());
  }

  /** Appends immutable raw Alpaca deliveries; downstream views can use payload_hash to deduplicate replays. */
  public int append(List<AlpacaBar> input) throws Exception {
    if (input.isEmpty()) return 0;
    List<AlpacaBar> bars = new ArrayList<>(input);

    String path = table.location() + "/data/batch-" + Instant.now().toEpochMilli() + ".parquet";
    OutputFile output = table.io().newOutputFile(path);
    long size;
    try (FileAppender<Record> appender = Parquet.write(output).schema(SCHEMA)
        .createWriterFunc(GenericParquetWriter::buildWriter).build()) {
      String ingestedAt = Instant.now().toString();
      for (AlpacaBar bar : bars) appender.add(record(bar, ingestedAt));
      size = appender.length();
    }
    DataFile file = DataFiles.builder(table.spec()).withPath(path).withFileSizeInBytes(size)
        .withRecordCount(bars.size()).build();
    table.newAppend().appendFile(file).commit();
    return bars.size();
  }

  private static Record record(AlpacaBar bar, String ingestedAt) {
    Record row = GenericRecord.create(SCHEMA);
    row.setField("payload_hash", hash(bar)); row.setField("T", bar.T()); row.setField("S", bar.S());
    row.setField("o", bar.o()); row.setField("h", bar.h()); row.setField("l", bar.l()); row.setField("c", bar.c());
    row.setField("v", bar.v()); row.setField("t", bar.t()); row.setField("n", bar.n()); row.setField("vw", bar.vw());
    row.setField("ingested_at", ingestedAt);
    return row;
  }

  static String hash(AlpacaBar bar) {
    try {
      String source = String.join("|", bar.T(), bar.S(), Double.toString(bar.o()), Double.toString(bar.h()),
          Double.toString(bar.l()), Double.toString(bar.c()), Long.toString(bar.v()), bar.t(), Long.toString(bar.n()), Double.toString(bar.vw()));
      return java.util.HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(source.getBytes(StandardCharsets.UTF_8)));
    } catch (Exception error) { throw new IllegalStateException(error); }
  }

  @Override public void close() {
    if (catalog instanceof AutoCloseable closeable) {
      try {
        closeable.close();
      } catch (Exception error) {
        throw new IllegalStateException("could not close Iceberg catalog", error);
      }
    }
  }
}
