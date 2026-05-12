package io.github.stellhub.stellflow.controller.replica;

import io.github.stellhub.stellflow.server.runtime.ReplicaManager;
import io.github.stellhub.stellflow.storage.log.LogManager;
import java.util.List;
import java.util.Optional;

/**
 * Controller/Replica 协调适配层。
 */
public class ControllerReplicaCoordinator {

    private final LogManager logManager;
    private final ReplicaManager replicaManager;

    public ControllerReplicaCoordinator(LogManager logManager) {
        this.logManager = logManager;
        this.replicaManager = null;
    }

    public ControllerReplicaCoordinator(ReplicaManager replicaManager) {
        this.logManager = replicaManager.logManager();
        this.replicaManager = replicaManager;
    }

    /**
     * 应用分区控制命令。
     */
    public PartitionControlApplyReport apply(PartitionControlCommand command) {
        if (replicaManager != null) {
            return replicaManager.applyPartitionControl(command);
        }
        long appliedAtMs = System.currentTimeMillis();
        try {
            if (command.deletePartition()) {
                logManager.deletePartition(command.topic(), command.partition());
                return new PartitionControlApplyReport(
                        command.topic(),
                        command.partition(),
                        command.leaderEpoch(),
                        true,
                        "deleted",
                        appliedAtMs,
                        true);
            }
            logManager.updateLeaderEpoch(command.topic(), command.partition(), command.leaderEpoch());
            List<Integer> replicaNodes =
                    command.replicaNodes() == null || command.replicaNodes().isEmpty()
                            ? List.of(command.leaderId())
                            : command.replicaNodes();
            List<Integer> isrNodes =
                    command.isrNodes() == null || command.isrNodes().isEmpty()
                            ? List.of(command.leaderId())
                            : command.isrNodes();
            logManager.updateReplicaTopology(
                    command.topic(),
                    command.partition(),
                    command.leaderId(),
                    replicaNodes,
                    isrNodes);
            if (command.truncateToLeaderEpoch() != null) {
                logManager.truncateToLeaderEpoch(
                        command.topic(), command.partition(), command.truncateToLeaderEpoch());
            }
            if (command.truncateToOffset() != null) {
                logManager.truncateTo(command.topic(), command.partition(), command.truncateToOffset());
            }
            return new PartitionControlApplyReport(
                    command.topic(),
                    command.partition(),
                    command.leaderEpoch(),
                    true,
                    "applied",
                    appliedAtMs,
                    false);
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
     * 删除本地单分区数据。
     */
    public void deleteLocalPartition(String topic, int partition) {
        logManager.deletePartition(topic, partition);
    }

    /**
     * 查询单分区复制进度快照。
     */
    public Optional<PartitionReplicaProgress> partitionProgress(String topic, int partition) {
        if (!logManager.containsPartition(topic, partition)) {
            return Optional.empty();
        }
        return Optional.of(
                new PartitionReplicaProgress(
                        topic,
                        partition,
                        logManager.leaderId(topic, partition),
                        logManager.leaderEpoch(topic, partition),
                        logManager.logEndOffset(topic, partition),
                        logManager.highWatermark(topic, partition),
                        logManager.replicaNodes(topic, partition),
                        logManager.isrNodes(topic, partition)));
    }

    /**
     * 查询指定副本的复制进度。
     */
    public long replicaEndOffset(String topic, int partition, int brokerId) {
        return logManager.replicaEndOffset(topic, partition, brokerId);
    }
}
