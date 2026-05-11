package io.github.stellhub.stellflow.controller.replica;

import io.github.stellhub.stellflow.network.protocol.FetchPartitionResponse;
import io.github.stellhub.stellflow.storage.log.LogManager;

/**
 * Follower 侧复制结果应用器。
 */
public class ReplicaFollowerApplier {

    private final LogManager logManager;

    public ReplicaFollowerApplier(LogManager logManager) {
        this.logManager = logManager;
    }

    /**
     * 应用 leader 返回的 replica fetch 结果。
     */
    public void apply(
            String topic,
            int partition,
            int leaderEpoch,
            FetchPartitionResponse partitionResponse) {
        logManager.appendReplicaEntries(
                topic,
                partition,
                ReplicaPayloadCodec.decode(partitionResponse.records()),
                leaderEpoch);
    }
}
