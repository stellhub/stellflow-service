package io.github.stellhub.stellflow.storage.log;

import java.util.List;

/**
 * 副本同步读取结果。
 */
public record ReplicaLogReadResult(
        long highWatermark,
        long logStartOffset,
        long lastStableOffset,
        long nextFetchOffset,
        List<ReplicaLogEntry> entries) {}
