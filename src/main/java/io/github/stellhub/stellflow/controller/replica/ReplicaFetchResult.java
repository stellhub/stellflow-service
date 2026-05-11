package io.github.stellhub.stellflow.controller.replica;

import io.github.stellhub.stellflow.network.protocol.ErrorCode;

/**
 * 单次副本抓取结果。
 */
public record ReplicaFetchResult(
        ErrorCode errorCode,
        int leaderEpoch,
        long highWatermark,
        long logStartOffset,
        long lastStableOffset,
        byte[] records) {}
