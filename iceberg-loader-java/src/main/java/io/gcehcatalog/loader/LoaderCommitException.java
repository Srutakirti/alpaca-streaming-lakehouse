package io.gcehcatalog.loader;

/** A loader write failure with enough durability context for safe operational handling. */
final class LoaderCommitException extends Exception {
  enum Type {
    DATA_FILE_WRITE_FAILED("data_file_write_failed"),
    ICEBERG_COMMIT_FAILED("iceberg_commit_failed"),
    ICEBERG_COMMIT_STATE_UNKNOWN("iceberg_commit_state_unknown"),
    OFFSET_COMMIT_FAILED("offset_commit_failed");

    private final String eventName;

    Type(String eventName) {
      this.eventName = eventName;
    }

    String eventName() {
      return eventName;
    }
  }

  private final Type type;
  private final String icebergCommitStatus;
  private final String offsetCommitStatus;

  LoaderCommitException(
      Type type,
      String icebergCommitStatus,
      String offsetCommitStatus,
      Throwable cause) {
    super(type.eventName(), cause);
    this.type = type;
    this.icebergCommitStatus = icebergCommitStatus;
    this.offsetCommitStatus = offsetCommitStatus;
  }

  Type type() {
    return type;
  }

  String icebergCommitStatus() {
    return icebergCommitStatus;
  }

  String offsetCommitStatus() {
    return offsetCommitStatus;
  }
}
