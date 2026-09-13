package io.gcehcatalog.loader;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

final class BatchWindowTest {
  @Test
  void excludesIdleTimeBeforeTheBatchOpens() {
    AtomicLong now = new AtomicLong();
    BatchWindow window = new BatchWindow(now::get);

    now.set(Duration.ofHours(1).toNanos());
    assertFalse(window.expired(Duration.ofMinutes(5)));

    window.open();
    now.addAndGet(Duration.ofMinutes(4).toNanos());
    assertFalse(window.expired(Duration.ofMinutes(5)));
    now.addAndGet(Duration.ofMinutes(1).toNanos());
    assertTrue(window.expired(Duration.ofMinutes(5)));
  }

  @Test
  void resetStartsANewWindowOnlyForARemainder() {
    AtomicLong now = new AtomicLong();
    BatchWindow window = new BatchWindow(now::get);
    window.open();
    now.addAndGet(Duration.ofMinutes(5).toNanos());
    assertTrue(window.expired(Duration.ofMinutes(5)));

    window.reset(true);
    assertFalse(window.expired(Duration.ofMinutes(5)));
    window.reset(false);
    now.addAndGet(Duration.ofHours(1).toNanos());
    assertFalse(window.expired(Duration.ofMinutes(5)));
  }
}
