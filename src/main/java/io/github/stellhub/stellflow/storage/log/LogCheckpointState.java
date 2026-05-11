package io.github.stellhub.stellflow.storage.log;

/**
 * checkpoint 状态快照。
 */
public record LogCheckpointState(
        long highWatermark,
        long logEndOffset,
        long recoveryPoint,
        long logStartOffset,
        int leaderEpoch) {}
