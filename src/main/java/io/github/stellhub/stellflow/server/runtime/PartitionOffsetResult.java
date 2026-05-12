package io.github.stellhub.stellflow.server.runtime;

import io.github.stellhub.stellflow.network.protocol.ErrorCode;
import java.util.List;

/**
 * 分区 offset 查询结果。
 */
public record PartitionOffsetResult(
        ErrorCode errorCode,
        int leaderEpoch,
        long timestamp,
        long offset,
        List<Long> offsets) {

    public PartitionOffsetResult {
        offsets = offsets == null ? List.of() : List.copyOf(offsets);
    }
}
