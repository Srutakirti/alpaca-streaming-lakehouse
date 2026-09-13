package io.gcehcatalog.loader;

import java.time.Duration;
import java.util.function.LongSupplier;

/** Monotonic clock for a non-empty batch; time spent with an empty batch is excluded. */
final class BatchWindow {
  private static final long CLOSED = -1;

  private final LongSupplier nanoTime;
  private long openedAt = CLOSED;

  BatchWindow() {
    this(System::nanoTime);
  }

  BatchWindow(LongSupplier nanoTime) {
    this.nanoTime = nanoTime;
  }

  void open() {
    if (openedAt == CLOSED) openedAt = nanoTime.getAsLong();
  }

  void reset(boolean hasRemainder) {
    openedAt = hasRemainder ? nanoTime.getAsLong() : CLOSED;
  }

  void close() {
    openedAt = CLOSED;
  }

  boolean expired(Duration maximumAge) {
    return openedAt != CLOSED
        && Duration.ofNanos(nanoTime.getAsLong() - openedAt).compareTo(maximumAge) >= 0;
  }
}
