package io.github.stellhub.stellflow.storage.log;

/**
 * 副本同步日志条目。
 */
public record ReplicaLogEntry(long offset, long timestamp, int leaderEpoch, byte[] records) {}
