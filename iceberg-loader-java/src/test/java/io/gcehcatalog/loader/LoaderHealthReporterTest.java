package io.gcehcatalog.loader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZoneId;
import java.util.Map;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

final class LoaderHealthReporterTest {
  private static final Instant NOW = Instant.parse("2026-09-13T12:00:00Z");

  @Test
  void statePrecedenceDistinguishesBacklogBufferAndIdle() {
    Duration stall = Duration.ofMinutes(15);
    Instant recent = NOW.minus(Duration.ofMinutes(2));

    assertEquals(LoaderHealthReporter.State.STARTING,
        state(false, false, 0, 0, 0, null, null));
    assertEquals(LoaderHealthReporter.State.COMMITTING,
        state(true, true, 20, 10, 1, recent, recent));
    assertEquals(LoaderHealthReporter.State.STALLED,
        state(true, false, 20, 10, 1, NOW.minus(stall), NOW.minus(stall)));
    assertEquals(LoaderHealthReporter.State.CATCHING_UP,
        state(true, false, 20, 10, 1, recent, recent));
    assertEquals(LoaderHealthReporter.State.BUFFERING,
        state(true, false, 1, 10, 1, recent, recent));
    assertEquals(LoaderHealthReporter.State.IDLE,
        state(true, false, 0, 0, 0, null, recent));
  }

  @Test
  void structuredFailureContainsDurabilityWithoutExceptionMessage() throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    LoaderHealthReporter reporter = reporter(bytes);
    LoaderCommitException failure = new LoaderCommitException(
        LoaderCommitException.Type.OFFSET_COMMIT_FAILED,
        "committed",
        "not_committed",
        new IllegalStateException("secret path must not be logged"));

    reporter.commitFailed(failure, Duration.ofSeconds(3));

    String line = bytes.toString(StandardCharsets.UTF_8).trim();
    assertTrue(line.startsWith(LoaderHealthReporter.PREFIX));
    JsonNode payload = json(line);
    assertEquals("ERROR", payload.get("level").asText());
    assertEquals("commit_failed", payload.at("/fields/event").asText());
    assertEquals("offset_commit_failed", payload.at("/fields/failure_type").asText());
    assertEquals("committed", payload.at("/fields/iceberg_commit_status").asText());
    assertEquals("not_committed", payload.at("/fields/offset_commit_status").asText());
    assertFalse(line.contains("secret path"));
  }

  @Test
  void startingHeartbeatUsesUtcAndContainsNoOffsetsBeforeAssignment() throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    reporter(bytes).starting();

    JsonNode payload = json(bytes.toString(StandardCharsets.UTF_8).trim());
    assertEquals("2026-09-13T12:00:00Z", payload.get("timestamp").asText());
    assertEquals("starting", payload.at("/fields/state").asText());
    assertEquals("bars", payload.at("/fields/topic").asText());
    assertFalse(payload.at("/fields").has("partitions"));
  }

  @Test
  void stalledTransitionIsReportedOnceWhileHeartbeatsContinue() {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    MutableClock clock = new MutableClock(NOW);
    LoaderHealthReporter reporter = new LoaderHealthReporter(
        "bars",
        Duration.ofMinutes(1),
        Duration.ofMinutes(15),
        clock,
        new PrintStream(bytes, true, StandardCharsets.UTF_8));
    MockConsumer<String, byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    TopicPartition partition = new TopicPartition("bars", 0);
    consumer.assign(java.util.List.of(partition));
    consumer.updateBeginningOffsets(Map.of(partition, 0L));
    consumer.updateEndOffsets(Map.of(partition, 10L));
    consumer.commitSync(Map.of(partition, new OffsetAndMetadata(0L)));
    BoundedBatch batch = new BoundedBatch(1000);

    reporter.maybeHeartbeat(consumer, batch);
    clock.advance(Duration.ofMinutes(15));
    reporter.maybeHeartbeat(consumer, batch);
    clock.advance(Duration.ofMinutes(1));
    reporter.maybeHeartbeat(consumer, batch);

    String output = bytes.toString(StandardCharsets.UTF_8);
    assertEquals(1, occurrences(output, "\"event\":\"stalled\""));
    assertEquals(3, occurrences(output, "\"event\":\"heartbeat\""));
  }

  private static LoaderHealthReporter reporter(ByteArrayOutputStream bytes) {
    return new LoaderHealthReporter(
        "bars",
        Duration.ofMinutes(1),
        Duration.ofMinutes(15),
        Clock.fixed(NOW, ZoneOffset.UTC),
        new PrintStream(bytes, true, StandardCharsets.UTF_8));
  }

  private static JsonNode json(String line) throws Exception {
    return new ObjectMapper().readTree(line.substring(LoaderHealthReporter.PREFIX.length()));
  }

  private static LoaderHealthReporter.State state(
      boolean initialized,
      boolean committing,
      long lag,
      int bars,
      int sources,
      Instant lagSince,
      Instant progressAt) {
    return LoaderHealthReporter.inferState(
        initialized,
        committing,
        lag,
        bars,
        sources,
        lagSince,
        progressAt,
        NOW,
        Duration.ofMinutes(15));
  }

  private static int occurrences(String text, String pattern) {
    return (text.length() - text.replace(pattern, "").length()) / pattern.length();
  }

  private static final class MutableClock extends Clock {
    private Instant instant;

    private MutableClock(Instant instant) {
      this.instant = instant;
    }

    void advance(Duration duration) {
      instant = instant.plus(duration);
    }

    @Override
    public ZoneId getZone() {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
      return this;
    }

    @Override
    public Instant instant() {
      return instant;
    }
  }
}
