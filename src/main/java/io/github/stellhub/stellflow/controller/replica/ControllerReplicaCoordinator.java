package io.github.stellhub.stellflow.controller.replica;

import io.github.stellhub.stellflow.storage.log.LogManager;
import java.util.List;

/**
 * Controller/Replica 协调适配层。
 */
public class ControllerReplicaCoordinator {

    private final LogManager logManager;

    public ControllerReplicaCoordinator(LogManager logManager) {
        this.logManager = logManager;
    }

    /**
     * 应用分区控制命令。
     */
    public PartitionControlApplyReport apply(PartitionControlCommand command) {
        long appliedAtMs = System.currentTimeMillis();
        try {
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
                    appliedAtMs);
        } catch (RuntimeException exception) {
            return new PartitionControlApplyReport(
                    command.topic(),
                    command.partition(),
                    command.leaderEpoch(),
                    false,
                    exception.getMessage(),
                    appliedAtMs);
        }
    }
}
