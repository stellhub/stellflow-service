package io.github.stellhub.stellflow.controller.replica;

import io.github.stellhub.stellflow.observability.metrics.ReplicaFetchMetrics;
import io.github.stellhub.stellflow.storage.log.LogManager;
import io.github.stellhub.stellflow.storage.log.ReplicaLogEntry;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

/**
 * 单分区 follower 后台拉取循环。
 */
@Slf4j
public class ReplicaFetchTask implements Runnable {

    private final ReplicaFetchAssignment assignment;
    private final ReplicaFetchConfig config;
    private final LogManager followerLogManager;
    private final ReplicaFollowerApplier followerApplier;
    private final ReplicaFetchConnectionPool connectionPool;
    private final ReplicaFetchMetrics metrics;

    public ReplicaFetchTask(
            ReplicaFetchAssignment assignment,
            ReplicaFetchConfig config,
            LogManager followerLogManager,
            ReplicaFollowerApplier followerApplier,
            ReplicaFetchConnectionPool connectionPool,
            ReplicaFetchMetrics metrics) {
        this.assignment = assignment;
        this.config = config;
        this.followerLogManager = followerLogManager;
        this.followerApplier = followerApplier;
        this.connectionPool = connectionPool;
        this.metrics = metrics;
    }

    @Override
    public void run() {
        for (int round = 0; round < config.getPipelineRoundsPerPoll(); round++) {
            long beforeFetchOffset =
                    followerLogManager.logEndOffset(assignment.topic(), assignment.partition());
            int currentLeaderEpoch =
                    followerLogManager.leaderEpoch(assignment.topic(), assignment.partition());
            long nowMs = System.currentTimeMillis();
            try {
                ReplicaFetchNetworkClient networkClient = connectionPool.get(assignment);
                ReplicaFetchResult result =
                        networkClient.fetch(
                                assignment.topic(),
                                assignment.partition(),
                                currentLeaderEpoch,
                                beforeFetchOffset);
                if (result.errorCode()
                        != io.github.stellhub.stellflow.network.protocol.ErrorCode.NONE) {
                    log.warn(
                            "Replica fetch returned error topic={} partition={} leader={} error={}",
                            assignment.topic(),
                            assignment.partition(),
                            assignment.leaderBrokerId(),
                            result.errorCode());
                    metrics.recordFailure(
                            assignment.topic(),
                            assignment.partition(),
                            assignment.leaderBrokerId(),
                            Math.max(0, result.highWatermark() - beforeFetchOffset),
                            nowMs);
                    return;
                }
                if (result.records() != null && result.records().length > 0) {
                    int applyLeaderEpoch =
                            Math.max(
                                    currentLeaderEpoch,
                                    followerLogManager.leaderEpoch(
                                            assignment.topic(), assignment.partition()));
                    followerApplier.apply(
                            assignment.topic(),
                            assignment.partition(),
                            applyLeaderEpoch,
                            new io.github.stellhub.stellflow.network.protocol.FetchPartitionResponse(
                                    assignment.partition(),
                                    io.github.stellhub.stellflow.network.protocol.ErrorCode.NONE,
                                    result.highWatermark(),
                                    result.logStartOffset(),
                                    result.lastStableOffset(),
                                    List.of(),
                                    result.records()));
                }
                List<ReplicaLogEntry> entries = ReplicaPayloadCodec.decode(result.records());
                long afterFetchOffset =
                        followerLogManager.logEndOffset(assignment.topic(), assignment.partition());
                metrics.recordSuccess(
                        assignment.topic(),
                        assignment.partition(),
                        assignment.leaderBrokerId(),
                        result.records() == null ? 0 : result.records().length,
                        entries.size(),
                        Math.max(0, result.highWatermark() - afterFetchOffset),
                        nowMs);
                if (afterFetchOffset > beforeFetchOffset) {
                    log.debug(
                            "Replica fetch applied topic={} partition={} fetched={} lag={}",
                            assignment.topic(),
                            assignment.partition(),
                            afterFetchOffset - beforeFetchOffset,
                            Math.max(0, result.highWatermark() - afterFetchOffset));
                }
                if (afterFetchOffset == beforeFetchOffset || entries.isEmpty()) {
                    return;
                }
            } catch (RuntimeException exception) {
                log.warn(
                        "Replica fetch loop failed topic={} partition={} leader={}:{}",
                        assignment.topic(),
                        assignment.partition(),
                        assignment.leaderHost(),
                        assignment.leaderPort(),
                        exception);
                metrics.recordFailure(
                        assignment.topic(),
                        assignment.partition(),
                        assignment.leaderBrokerId(),
                        0,
                        nowMs);
                return;
            }
        }
    }

    /**
     * 任务关闭时无需单独关闭连接，连接由共享池托管。
     */
    public void close() {
        // Shared connection lifecycle is managed by ReplicaFetchConnectionPool.
    }
}
