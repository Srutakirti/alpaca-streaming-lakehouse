package io.gcehcatalog.loader;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.PrintStream;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/** Emits bounded, structured loader health without adding another process or metrics agent. */
final class LoaderHealthReporter {
  static final String PREFIX = "LOADER_HEALTH ";

  enum State {
    STARTING("starting"),
    COMMITTING("committing"),
    STALLED("stalled"),
    CATCHING_UP("catching_up"),
    BUFFERING("buffering"),
    IDLE("idle");

    private final String value;

    State(String value) {
      this.value = value;
    }

    String value() {
      return value;
    }
  }

  record PartitionOffsets(
      String topic, int partition, long committedOffset, long endOffset, long lagRecords) {}

  private static final ObjectMapper JSON = new ObjectMapper();

  private final String topic;
  private final Duration heartbeatInterval;
  private final Duration stallThreshold;
  private final Clock clock;
  private final PrintStream output;
  private final Instant processStartedAt;
  private final Map<TopicPartition, Long> previousCommittedOffsets = new LinkedHashMap<>();

  private Instant lastHeartbeatAt;
  private Instant lastInputAt;
  private Instant lastCommitAt;
  private Long lastCommitDurationMs;
  private Instant positiveLagSince;
  private Instant lastDurableProgressAt;
  private boolean committing;
  private State previousHeartbeatState;

  LoaderHealthReporter(String topic, Duration heartbeatInterval, Duration stallThreshold) {
    this(topic, heartbeatInterval, stallThreshold, Clock.systemUTC(), System.out);
  }

  LoaderHealthReporter(
      String topic,
      Duration heartbeatInterval,
      Duration stallThreshold,
      Clock clock,
      PrintStream output) {
    if (heartbeatInterval.isZero() || heartbeatInterval.isNegative()) {
      throw new IllegalArgumentException("LOADER_HEARTBEAT_SECONDS must be positive");
    }
    if (stallThreshold.isZero() || stallThreshold.isNegative()) {
      throw new IllegalArgumentException("LOADER_STALL_SECONDS must be positive");
    }
    this.topic = topic;
    this.heartbeatInterval = heartbeatInterval;
    this.stallThreshold = stallThreshold;
    this.clock = clock;
    this.output = output;
    this.processStartedAt = clock.instant();
  }

  void starting() {
    emit("heartbeat", "INFO", baseFields(State.STARTING));
    lastHeartbeatAt = clock.instant();
  }

  void recordInput() {
    lastInputAt = clock.instant();
  }

  void commitStarted(BoundedBatch.Batch batch) {
    committing = true;
    Map<String, Object> fields = baseFields(State.COMMITTING);
    fields.put("batch_bars", batch.barCount());
    fields.put("batch_source_records", batch.sourceRecordCount());
    emit("commit_started", "INFO", fields);
  }

  void commitSucceeded(Duration duration) {
    committing = false;
    lastCommitAt = clock.instant();
    lastCommitDurationMs = duration.toMillis();
    Map<String, Object> fields = baseFields(State.COMMITTING);
    fields.put("durable_commit_total_duration_ms", lastCommitDurationMs);
    emit("commit_succeeded", "INFO", fields);
  }

  void commitFailed(LoaderCommitException failure, Duration duration) {
    committing = false;
    Map<String, Object> fields = baseFields(State.COMMITTING);
    fields.put("failure_type", failure.type().eventName());
    fields.put("error_class", failure.getCause().getClass().getName());
    fields.put("iceberg_commit_status", failure.icebergCommitStatus());
    fields.put("offset_commit_status", failure.offsetCommitStatus());
    fields.put("durable_commit_total_duration_ms", duration.toMillis());
    emit("commit_failed", "ERROR", fields);
  }

  void loaderFailed(Exception failure) {
    Map<String, Object> fields = baseFields(committing ? State.COMMITTING : State.STARTING);
    fields.put("failure_type", "loader_failed");
    fields.put("error_class", failure.getClass().getName());
    fields.put("iceberg_commit_status", "unknown");
    fields.put("offset_commit_status", "unknown");
    emit("loader_failed", "ERROR", fields);
  }

  void maybeHeartbeat(
      Consumer<String, byte[]> consumer,
      BoundedBatch batch) {
    Instant now = clock.instant();
    if (lastHeartbeatAt != null
        && Duration.between(lastHeartbeatAt, now).compareTo(heartbeatInterval) < 0) {
      return;
    }
    try {
      List<PartitionOffsets> offsets = offsets(consumer);
      long lag = offsets.stream().mapToLong(PartitionOffsets::lagRecords).sum();
      updateProgress(offsets, lag, now);
      State state = inferState(
          !offsets.isEmpty(),
          committing,
          lag,
          batch.barCount(),
          batch.sourceRecordCount(),
          positiveLagSince,
          lastDurableProgressAt,
          now,
          stallThreshold);
      Map<String, Object> fields = baseFields(state);
      fields.put("partitions", offsets);
      fields.put("aggregate_lag_records", lag);
      fields.put("buffered_bars", batch.barCount());
      fields.put("buffered_source_records", batch.sourceRecordCount());
      if (state == State.STALLED && previousHeartbeatState != State.STALLED) {
        emit("stalled", "ERROR", new LinkedHashMap<>(fields));
      }
      emit("heartbeat", "INFO", fields);
      previousHeartbeatState = state;
      lastHeartbeatAt = now;
    } catch (RuntimeException error) {
      output.printf("loader_health_sample_failed error_class=%s%n", error.getClass().getName());
      lastHeartbeatAt = now;
    }
  }

  private List<PartitionOffsets> offsets(Consumer<String, byte[]> consumer) {
    Set<TopicPartition> assignment = consumer.assignment();
    if (assignment.isEmpty()) return List.of();
    Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(assignment);
    Set<TopicPartition> missingCommittedOffset = new HashSet<>();
    for (TopicPartition partition : assignment) {
      if (committed.get(partition) == null) missingCommittedOffset.add(partition);
    }
    Map<TopicPartition, Long> beginning = missingCommittedOffset.isEmpty()
        ? Map.of()
        : consumer.beginningOffsets(missingCommittedOffset);
    Map<TopicPartition, Long> end = consumer.endOffsets(assignment);
    List<PartitionOffsets> result = new ArrayList<>();
    for (TopicPartition partition : assignment) {
      OffsetAndMetadata committedMetadata = committed.get(partition);
      long committedOffset = committedMetadata == null
          ? beginning.getOrDefault(partition, 0L)
          : committedMetadata.offset();
      long endOffset = end.getOrDefault(partition, committedOffset);
      result.add(new PartitionOffsets(
          partition.topic(),
          partition.partition(),
          committedOffset,
          endOffset,
          Math.max(0, endOffset - committedOffset)));
    }
    result.sort(Comparator.comparing(PartitionOffsets::topic)
        .thenComparingInt(PartitionOffsets::partition));
    return result;
  }

  private boolean updateProgress(List<PartitionOffsets> offsets, long lag, Instant now) {
    boolean progressed = offsets.stream().anyMatch(offset -> {
      Long previous = previousCommittedOffsets.get(
          new TopicPartition(offset.topic(), offset.partition()));
      return previous != null && offset.committedOffset() > previous;
    });
    previousCommittedOffsets.clear();
    for (PartitionOffsets offset : offsets) {
      previousCommittedOffsets.put(
          new TopicPartition(offset.topic(), offset.partition()), offset.committedOffset());
    }
    if (lag == 0) {
      positiveLagSince = null;
      lastDurableProgressAt = now;
    } else {
      if (positiveLagSince == null) positiveLagSince = now;
      if (progressed) lastDurableProgressAt = now;
    }
    return progressed;
  }

  static State inferState(
      boolean initialized,
      boolean committing,
      long lagRecords,
      int bufferedBars,
      int bufferedSourceRecords,
      Instant positiveLagSince,
      Instant lastDurableProgressAt,
      Instant now,
      Duration stallThreshold) {
    if (!initialized) return State.STARTING;
    if (committing) return State.COMMITTING;
    Instant progressBoundary = later(positiveLagSince, lastDurableProgressAt);
    if (lagRecords > 0
        && progressBoundary != null
        && Duration.between(progressBoundary, now).compareTo(stallThreshold) >= 0) {
      return State.STALLED;
    }
    if (lagRecords > bufferedSourceRecords) return State.CATCHING_UP;
    if (bufferedBars > 0 || bufferedSourceRecords > 0) return State.BUFFERING;
    return State.IDLE;
  }

  private static Instant later(Instant left, Instant right) {
    if (left == null) return right;
    if (right == null) return left;
    return left.isAfter(right) ? left : right;
  }

  private Map<String, Object> baseFields(State state) {
    Map<String, Object> fields = new LinkedHashMap<>();
    fields.put("state", state.value());
    fields.put("process_started_at_utc", processStartedAt.toString());
    fields.put("topic", topic);
    fields.put("last_input_at_utc", text(lastInputAt));
    fields.put("last_commit_at_utc", text(lastCommitAt));
    fields.put("last_commit_duration_ms", lastCommitDurationMs);
    return fields;
  }

  private void emit(String event, String level, Map<String, Object> fields) {
    Map<String, Object> payload = new LinkedHashMap<>();
    payload.put("timestamp", clock.instant().toString());
    payload.put("level", level);
    payload.put("target", "iceberg_loader::health");
    fields.put("event", event);
    payload.put("fields", fields);
    try {
      output.println(PREFIX + JSON.writeValueAsString(payload));
      output.flush();
    } catch (Exception error) {
      output.printf("loader_health_serialization_failed error_class=%s%n", error.getClass().getName());
    }
  }

  private static String text(Instant value) {
    return value == null ? null : value.toString();
  }
}
