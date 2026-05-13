package io.github.stellhub.stellflow.server.runtime;

import io.github.stellhub.stellflow.controller.replica.PartitionControlApplyReport;
import io.github.stellhub.stellflow.controller.replica.PartitionControlCommand;
import io.github.stellhub.stellflow.metadata.MetadataCache;
import io.github.stellhub.stellflow.metadata.PartitionMetadata;
import io.github.stellhub.stellflow.metadata.PartitionRole;
import io.github.stellhub.stellflow.network.protocol.ErrorCode;
import io.github.stellhub.stellflow.storage.log.LogManager;
import io.github.stellhub.stellflow.storage.log.ReplicaLogEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Broker 副本与分区运行时统一入口。
 */
public class ReplicaManager {

    private final LogManager logManager;
    private final MetadataCache metadataCache;
    private final boolean allowAutoCreateForCompatibility;

    public ReplicaManager(
            LogManager logManager, MetadataCache metadataCache, boolean allowAutoCreateForCompatibility) {
        this.logManager = logManager;
        this.metadataCache = metadataCache;
        this.allowAutoCreateForCompatibility = allowAutoCreateForCompatibility;
    }

    /**
     * 追加 producer 数据。
     */
    public PartitionAppendResult append(String topic, int partition, byte[] records) {
        return append(topic, partition, records, (short) 1, 0);
    }

    /**
     * 按指定 acks 语义追加 producer 数据。
     */
    public PartitionAppendResult append(
            String topic, int partition, byte[] records, short acks, int timeoutMs) {
        Optional<PartitionManager> partitionManager = partitionManager(topic, partition);
        if (partitionManager.isEmpty()) {
            if (!allowAutoCreateForCompatibility) {
                return new PartitionAppendResult(
                        ErrorCode.UNKNOWN_TOPIC_OR_PARTITION, -1, 0, 0, 0, 0, 0);
            }
            createLocalPartition(topic, partition, 0);
            partitionManager = partitionManager(topic, partition);
        }
        return partitionManager
                .map(manager -> manager.appendAsLeader(records, acks, timeoutMs))
                .orElseGet(
                        () ->
                                new PartitionAppendResult(
                                        ErrorCode.UNKNOWN_TOPIC_OR_PARTITION, -1, 0, 0, 0, 0, 0));
    }

    /**
     * 普通 consumer 读取。
     */
    public PartitionReadResult readConsumer(String topic, int partition, long fetchOffset, int maxBytes) {
        return partitionManager(topic, partition)
                .map(manager -> manager.readConsumer(fetchOffset, maxBytes))
                .orElseGet(() -> unknownRead());
    }

    /**
     * Follower 副本读取。
     */
    public PartitionReadResult readReplica(
            String topic, int partition, int replicaId, long fetchOffset, int maxBytes) {
        return partitionManager(topic, partition)
                .map(manager -> manager.readReplica(replicaId, fetchOffset, maxBytes))
                .orElseGet(() -> unknownRead());
    }

    /**
     * 查询分区 offset。
     */
    public PartitionOffsetResult listOffsets(
            String topic, int partition, long timestamp, int currentLeaderEpoch, int maxNumOffsets) {
        return partitionManager(topic, partition)
                .map(manager -> manager.listOffsets(timestamp, currentLeaderEpoch, maxNumOffsets))
                .orElseGet(
                        () -> new PartitionOffsetResult(
                                ErrorCode.UNKNOWN_TOPIC_OR_PARTITION, 0, 0, 0, List.of()));
    }

    /**
     * 创建本地 topic。
     */
    public List<TopicControlResult> createTopic(String topic, int partitionCount) {
        if (partitionCount <= 0) {
            return List.of(new TopicControlResult(topic, -1, ErrorCode.INVALID_REQUEST, 0));
        }
        List<TopicControlResult> results = new ArrayList<>();
        metadataCache.createLocalTopic(topic, partitionCount);
        for (int partition = 0; partition < partitionCount; partition++) {
            createLocalPartition(topic, partition, 0);
            results.add(new TopicControlResult(topic, partition, ErrorCode.NONE, 0));
        }
        return results;
    }

    /**
     * 删除本地 topic。
     */
    public List<TopicControlResult> deleteTopic(String topic) {
        if (!metadataCache.containsTopic(topic) && !logManager.containsTopic(topic)) {
            return List.of(new TopicControlResult(topic, -1, ErrorCode.UNKNOWN_TOPIC_OR_PARTITION, 0));
        }
        List<TopicControlResult> results = new ArrayList<>();
        for (Integer partition : logManager.partitions(topic)) {
            results.add(new TopicControlResult(topic, partition, ErrorCode.NONE, 0));
        }
        logManager.deleteTopic(topic);
        metadataCache.deleteTopic(topic);
        return results.isEmpty()
                ? List.of(new TopicControlResult(topic, -1, ErrorCode.NONE, 0))
                : results;
    }

    /**
     * 更新单个分区拓扑。
     */
    public TopicControlResult alterPartition(
            String topic, int partition, int leaderId, int leaderEpoch, List<Integer> replicas, List<Integer> isr) {
        PartitionMetadata metadata =
                new PartitionMetadata(
                        topic,
                        partition,
                        leaderId,
                        leaderEpoch,
                        replicas,
                        isr,
                        leaderId == metadataCache.localBrokerId()
                                ? PartitionRole.LEADER
                                : replicas.contains(metadataCache.localBrokerId())
                                        ? PartitionRole.FOLLOWER
                                        : PartitionRole.OFFLINE);
        metadataCache.upsertPartition(metadata);
        logManager.updateLeaderEpoch(topic, partition, leaderEpoch);
        logManager.updateReplicaTopology(topic, partition, leaderId, replicas, isr);
        return new TopicControlResult(topic, partition, ErrorCode.NONE, leaderEpoch);
    }

    /**
     * 应用 controller 下发的分区控制命令。
     */
    public PartitionControlApplyReport applyPartitionControl(PartitionControlCommand command) {
        long appliedAtMs = System.currentTimeMillis();
        try {
            if (command.deletePartition()) {
                logManager.deletePartition(command.topic(), command.partition());
                metadataCache.deletePartition(command.topic(), command.partition());
                return report(command, true, "deleted", appliedAtMs);
            }
            alterPartition(
                    command.topic(),
                    command.partition(),
                    command.leaderId(),
                    command.leaderEpoch(),
                    safeReplicas(command),
                    safeIsr(command));
            if (command.truncateToLeaderEpoch() != null) {
                logManager.truncateToLeaderEpoch(
                        command.topic(), command.partition(), command.truncateToLeaderEpoch());
            }
            if (command.truncateToOffset() != null) {
                logManager.truncateTo(command.topic(), command.partition(), command.truncateToOffset());
            }
            return report(command, true, "applied", appliedAtMs);
        } catch (RuntimeException exception) {
            return new PartitionControlApplyReport(
                    command.topic(),
                    command.partition(),
                    command.leaderEpoch(),
                    false,
                    exception.getMessage(),
                    appliedAtMs,
                    command.deletePartition());
        }
    }

    /**
     * Follower 追加 leader 同步的数据。
     */
    public void appendReplicaEntries(
            String topic, int partition, List<ReplicaLogEntry> entries, int leaderEpoch) {
        logManager.appendReplicaEntries(topic, partition, entries, leaderEpoch);
    }

    public LogManager logManager() {
        return logManager;
    }

    public MetadataCache metadataCache() {
        return metadataCache;
    }

    private Optional<PartitionManager> partitionManager(String topic, int partition) {
        Optional<PartitionMetadata> metadata = metadataCache.partition(topic, partition);
        if (metadata.isEmpty()
                && allowAutoCreateForCompatibility
                && logManager.containsPartition(topic, partition)) {
            metadataCache.upsertPartition(
                    new PartitionMetadata(
                            topic,
                            partition,
                            logManager.leaderId(topic, partition),
                            logManager.leaderEpoch(topic, partition),
                            logManager.replicaNodes(topic, partition),
                            logManager.isrNodes(topic, partition),
                            logManager.leaderId(topic, partition) == metadataCache.localBrokerId()
                                    ? PartitionRole.LEADER
                                    : PartitionRole.FOLLOWER));
            metadata = metadataCache.partition(topic, partition);
        }
        return metadata
                .map(value -> new PartitionManager(logManager, value));
    }

    private void createLocalPartition(String topic, int partition, int leaderEpoch) {
        alterPartition(
                topic,
                partition,
                metadataCache.localBrokerId(),
                leaderEpoch,
                List.of(metadataCache.localBrokerId()),
                List.of(metadataCache.localBrokerId()));
    }

    private List<Integer> safeReplicas(PartitionControlCommand command) {
        return command.replicaNodes() == null || command.replicaNodes().isEmpty()
                ? List.of(command.leaderId())
                : command.replicaNodes();
    }

    private List<Integer> safeIsr(PartitionControlCommand command) {
        return command.isrNodes() == null || command.isrNodes().isEmpty()
                ? List.of(command.leaderId())
                : command.isrNodes();
    }

    private PartitionControlApplyReport report(
            PartitionControlCommand command, boolean success, String message, long appliedAtMs) {
        return new PartitionControlApplyReport(
                command.topic(),
                command.partition(),
                command.leaderEpoch(),
                success,
                message,
                appliedAtMs,
                command.deletePartition());
    }

    private PartitionReadResult unknownRead() {
        return new PartitionReadResult(
                ErrorCode.UNKNOWN_TOPIC_OR_PARTITION, 0, 0, 0, 0, new byte[0], List.of());
    }
}
