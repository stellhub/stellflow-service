package io.github.stellhub.stellflow.network.protocol;

import java.util.List;

/**
 * SyncGroup 响应体。
 */
public record SyncGroupResponseBody(
        ErrorCode errorCode, List<ConsumerPartitionAssignment> assignments)
        implements ResponseBody {

    public SyncGroupResponseBody(ErrorCode errorCode) {
        this(errorCode, List.of());
    }
}
