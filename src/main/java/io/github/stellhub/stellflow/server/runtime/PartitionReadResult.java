package io.github.stellhub.stellflow.server.runtime;

import io.github.stellhub.stellflow.network.protocol.ErrorCode;
import io.github.stellhub.stellflow.storage.log.ReplicaLogEntry;
import java.util.List;

/**
 * 分区读取结果。
 */
public record PartitionReadResult(
        ErrorCode errorCode,
        long highWatermark,
        long logStartOffset,
        long lastStableOffset,
        long nextFetchOffset,
        byte[] records,
        List<ReplicaLogEntry> replicaEntries) {

    public PartitionReadResult {
        records = records == null ? new byte[0] : records;
        replicaEntries = replicaEntries == null ? List.of() : List.copyOf(replicaEntries);
    }
}
