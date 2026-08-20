package io.gcehcatalog.loader;

import java.util.ArrayList;
import java.util.List;

/**
 * Bounded in-memory write batches with source-record boundaries retained for
 * safe Kafka offset commits. A source record may span batches; its offset is
 * eligible for commit only in the batch containing its final bar.
 */
final class BoundedBatch {
  record Source(String topic, int partition, long offset) {}
  record Entry(AlpacaBar bar, Source source, boolean finalBarOfSource) {}
  record Batch(List<Entry> entries) {
    List<AlpacaBar> bars() { return entries.stream().map(Entry::bar).toList(); }
  }

  private final int maximumRecords;
  private final List<Entry> pending = new ArrayList<>();

  BoundedBatch(int maximumRecords) {
    if (maximumRecords < 1) throw new IllegalArgumentException("LOADER_MAX_RECORDS must be positive");
    this.maximumRecords = maximumRecords;
  }

  /** Adds one Kafka source record and returns every full batch it completed. */
  List<Batch> add(Source source, List<AlpacaBar> bars) {
    List<Batch> completed = new ArrayList<>();
    for (int index = 0; index < bars.size(); index++) {
      pending.add(new Entry(bars.get(index), source, index == bars.size() - 1));
      if (pending.size() == maximumRecords) completed.add(drain());
    }
    return completed;
  }

  boolean isEmpty() { return pending.isEmpty(); }

  Batch drain() {
    if (pending.isEmpty()) throw new IllegalStateException("cannot drain an empty batch");
    Batch batch = new Batch(List.copyOf(pending));
    pending.clear();
    return batch;
  }
}
