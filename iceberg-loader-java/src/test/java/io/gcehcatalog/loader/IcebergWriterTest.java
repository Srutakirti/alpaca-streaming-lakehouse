package io.gcehcatalog.loader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import java.nio.file.Files;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

class IcebergWriterTest {
  @Test void appendsAlpacaBarsAsImmutableRawDeliveries() throws Exception {
    String warehouse = Files.createTempDirectory("hcatalog-loader-test-").toUri().toString();
    AlpacaBar bar = new AlpacaBar("b", "AAPL", 175, 176, 174, 175.5, 1000, Instant.parse("2026-01-02T14:30:00Z").toString(), 10, 175.2);
    try (IcebergWriter writer = new IcebergWriter(new CatalogConfig(CatalogConfig.Type.HADOOP, "alpaca", warehouse, "", "", "", ""))) {
      assertEquals(1, writer.append(List.of(bar)));
      assertEquals(1, writer.append(List.of(bar)));
    }
  }
}
