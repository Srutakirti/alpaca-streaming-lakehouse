package io.gcehcatalog.loader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class BoundedBatchTest {
  @Test void splitsNineHundredPlusFiveHundredWithoutCommittingThePartialSource() {
    BoundedBatch buffer = new BoundedBatch(1000);
    BoundedBatch.Source first = new BoundedBatch.Source("bars", 0, 41);
    BoundedBatch.Source second = new BoundedBatch.Source("bars", 0, 42);

    assertTrue(buffer.add(first, bars(900)).isEmpty());
    List<BoundedBatch.Batch> completed = buffer.add(second, bars(500));

    assertEquals(1, completed.size());
    BoundedBatch.Batch firstWrite = completed.get(0);
    assertEquals(1000, firstWrite.entries().size());
    assertEquals(900, firstWrite.entries().stream().filter(entry -> entry.source().equals(first)).count());
    assertEquals(100, firstWrite.entries().stream().filter(entry -> entry.source().equals(second)).count());
    assertTrue(firstWrite.entries().stream().anyMatch(entry -> entry.source().equals(first) && entry.finalBarOfSource()));
    assertTrue(firstWrite.entries().stream().noneMatch(entry -> entry.source().equals(second) && entry.finalBarOfSource()));

    BoundedBatch.Batch remainder = buffer.drain();
    assertEquals(400, remainder.entries().size());
    assertTrue(remainder.entries().get(remainder.entries().size() - 1).finalBarOfSource());
    assertEquals(second, remainder.entries().get(remainder.entries().size() - 1).source());
  }

  @Test void splitsAnOversizedSourceArrayIntoBoundedWrites() {
    BoundedBatch buffer = new BoundedBatch(1000);
    BoundedBatch.Source source = new BoundedBatch.Source("bars", 0, 99);

    List<BoundedBatch.Batch> completed = buffer.add(source, bars(2500));

    assertEquals(2, completed.size());
    assertEquals(1000, completed.get(0).entries().size());
    assertEquals(1000, completed.get(1).entries().size());
    assertTrue(completed.stream().flatMap(batch -> batch.entries().stream()).noneMatch(BoundedBatch.Entry::finalBarOfSource));
    BoundedBatch.Batch remainder = buffer.drain();
    assertEquals(500, remainder.entries().size());
    assertTrue(remainder.entries().get(remainder.entries().size() - 1).finalBarOfSource());
  }

  private static List<AlpacaBar> bars(int count) {
    List<AlpacaBar> bars = new ArrayList<>();
    for (int index = 0; index < count; index++) {
      bars.add(new AlpacaBar("b", "AAPL", 1, 1, 1, 1, 1,
          Instant.parse("2026-01-02T14:30:00Z").plusSeconds(index).toString(), 1, 1));
    }
    return bars;
  }
}
