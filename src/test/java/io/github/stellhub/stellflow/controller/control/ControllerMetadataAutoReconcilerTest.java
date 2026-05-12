package io.github.stellhub.stellflow.controller.control;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.stellhub.stellflow.controller.replica.ControllerReplicaCoordinator;
import io.github.stellhub.stellflow.storage.log.LogManager;
import io.github.stellhub.stellflow.storage.log.LogStorageConfig;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * ControllerMetadataAutoReconciler 测试。
 */
class ControllerMetadataAutoReconcilerTest {

    @TempDir private Path tempDir;

    /**
     * 验证 stale broker 会被自动 fenced，fresh broker 会被自动 unfenced。
     */
    @Test
    void shouldAutomaticallyFenceAndUnfenceBrokersByHeartbeat() {
        ControllerAssignmentRegistry assignmentRegistry = new ControllerAssignmentRegistry();
        ControllerPartitionControlRegistry partitionControlRegistry =
                new ControllerPartitionControlRegistry();
        PartitionControlResultRegistry resultRegistry = new PartitionControlResultRegistry();
        ControllerMetadataStateMachine stateMachine =
                new ControllerMetadataStateMachine(
                        assignmentRegistry, partitionControlRegistry, resultRegistry);
        ControllerMetadataDecisionService decisionService =
                new ControllerMetadataDecisionService(
                        new DirectControllerMetadataCommandService(stateMachine), stateMachine);
        try (LogManager logManager =
                new LogManager(
                        LogStorageConfig.builder()
                                .rootDir(tempDir.resolve("heartbeat-log"))
                                .segmentBytes(1024)
                                .indexIntervalBytes(1)
                                .retentionSegments(8)
                                .retentionMs(7L * 24 * 60 * 60 * 1000)
                                .retentionBytes(1024 * 1024)
                                .build())) {
            ControllerMetadataAutoReconciler reconciler =
                    new ControllerMetadataAutoReconciler(
                            ControllerAutoReconcileConfig.builder()
                                    .enabled(true)
                                    .intervalMs(1000)
                                    .brokerHeartbeatTimeoutMs(200)
                                    .maxReplicaLagMessages(0)
                                    .build(),
                            stateMachine,
                            decisionService,
                            new ControllerReplicaCoordinator(logManager));

            long now = System.currentTimeMillis();
            decisionService.registerBroker(
                    1, "stellflow://127.0.0.1:9092", "127.0.0.1", 9092, now - 1000);
            reconciler.reconcileOnce();
            assertTrue(stateMachine.broker(1).orElseThrow().fenced());

            decisionService.registerBroker(
                    2, "stellflow://127.0.0.1:9093", "127.0.0.1", 9093, System.currentTimeMillis());
            stateMachine.fenceBroker(2);
            assertTrue(stateMachine.broker(2).orElseThrow().fenced());

            reconciler.reconcileOnce();
            assertFalse(stateMachine.broker(2).orElseThrow().fenced());
        }
    }

    /**
     * 验证副本进度会自动驱动 ISR shrink 与 expand。
     */
    @Test
    void shouldAutomaticallyShrinkAndExpandIsrByReplicaLag() {
        ControllerAssignmentRegistry assignmentRegistry = new ControllerAssignmentRegistry();
        ControllerPartitionControlRegistry partitionControlRegistry =
                new ControllerPartitionControlRegistry();
        PartitionControlResultRegistry resultRegistry = new PartitionControlResultRegistry();
        ControllerMetadataStateMachine stateMachine =
                new ControllerMetadataStateMachine(
                        assignmentRegistry, partitionControlRegistry, resultRegistry);
        ControllerMetadataDecisionService decisionService =
                new ControllerMetadataDecisionService(
                        new DirectControllerMetadataCommandService(stateMachine), stateMachine);
        try (LogManager logManager =
                new LogManager(
                        LogStorageConfig.builder()
                                .rootDir(tempDir.resolve("lag-log"))
                                .segmentBytes(1024)
                                .indexIntervalBytes(1)
                                .retentionSegments(8)
                                .retentionMs(7L * 24 * 60 * 60 * 1000)
                                .retentionBytes(1024 * 1024)
                                .build())) {
            ControllerReplicaCoordinator replicaCoordinator =
                    new ControllerReplicaCoordinator(logManager);
            ControllerMetadataAutoReconciler reconciler =
                    new ControllerMetadataAutoReconciler(
                            ControllerAutoReconcileConfig.builder()
                                    .enabled(true)
                                    .intervalMs(1000)
                                    .brokerHeartbeatTimeoutMs(5000)
                                    .maxReplicaLagMessages(0)
                                    .build(),
                            stateMachine,
                            decisionService,
                            replicaCoordinator);

            long now = System.currentTimeMillis();
            decisionService.registerBroker(
                    0, "stellflow://127.0.0.1:9092", "127.0.0.1", 9092, now);
            decisionService.registerBroker(
                    1, "stellflow://127.0.0.1:9093", "127.0.0.1", 9093, now);
            decisionService.createTopic("auto-isr", 1, 2);

            logManager.updateReplicaTopology("auto-isr", 0, 0, List.of(0, 1), List.of(0, 1));
            logManager.append("auto-isr", 0, "a".getBytes(StandardCharsets.UTF_8));
            logManager.append("auto-isr", 0, "b".getBytes(StandardCharsets.UTF_8));
            reconciler.reconcileOnce();

            assertEquals(List.of(0), stateMachine.partition("auto-isr", 0).orElseThrow().isrNodes());

            logManager.updateReplicaFetchOffset("auto-isr", 0, 1, 2L);
            reconciler.reconcileOnce();

            assertEquals(
                    List.of(0, 1), stateMachine.partition("auto-isr", 0).orElseThrow().isrNodes());
        }
    }
}
