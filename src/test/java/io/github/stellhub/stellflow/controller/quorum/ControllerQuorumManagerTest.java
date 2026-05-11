package io.github.stellhub.stellflow.controller.quorum;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import io.github.stellhub.stellflow.controller.control.ControllerAssignmentRegistry;
import io.github.stellhub.stellflow.controller.control.ControllerMetadataStateMachine;
import io.github.stellhub.stellflow.controller.control.ControllerPartitionControlRegistry;
import io.github.stellhub.stellflow.controller.control.PartitionControlResultRegistry;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * ControllerQuorumManager 测试。
 */
class ControllerQuorumManagerTest {

    @TempDir private Path tempDir;

    /**
     * 验证单节点 quorum 可提交 broker 注册和分区元数据。
     */
    @Test
    void shouldApplyCommittedMetadataRecordsThroughSingleNodeQuorum() throws Exception {
        int quorumPort = findFreePort();
        ControllerMetadataStateMachine metadataStateMachine =
                new ControllerMetadataStateMachine(
                        new ControllerAssignmentRegistry(),
                        new ControllerPartitionControlRegistry(),
                        new PartitionControlResultRegistry());
        ControllerQuorumConfig config =
                ControllerQuorumConfig.builder()
                        .enabled(true)
                        .selfId("c1")
                        .groupId("33333333-3333-3333-3333-333333333333")
                        .endpoint("grpc://127.0.0.1:" + quorumPort)
                        .storageDir(tempDir.resolve("controller-quorum"))
                        .peers("c1@grpc://127.0.0.1:" + quorumPort)
                        .requestTimeoutMs(3000)
                        .build();

        try (ControllerQuorumManager quorumManager =
                new ControllerQuorumManager(config, metadataStateMachine)) {
            quorumManager.start();
            quorumManager.registerBroker(
                    1, "stellflow://broker-a.example.com:9092", "broker-a.example.com", 9092, 10L);
            quorumManager.upsertPartition(
                    ControllerMetadataStateMachine.partition(
                            "orders",
                            0,
                            1,
                            7,
                            List.of(1, 2),
                            List.of(1, 2),
                            null,
                            null));

            waitUntil(() -> metadataStateMachine.brokers().size() == 1, 5000);
            waitUntil(() -> metadataStateMachine.partitions().size() == 1, 5000);

            assertEquals("broker-a.example.com", metadataStateMachine.brokers().get(0).advertisedHost());
            assertEquals(7, metadataStateMachine.partitions().get(0).leaderEpoch());
            assertFalse(metadataStateMachine.partitions().isEmpty());
        }
    }

    private static int findFreePort() throws IOException {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            return serverSocket.getLocalPort();
        }
    }

    private static void waitUntil(BooleanSupplier supplier, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (supplier.getAsBoolean()) {
                return;
            }
            Thread.sleep(50);
        }
        if (!supplier.getAsBoolean()) {
            throw new AssertionError("Condition was not satisfied within timeout");
        }
    }

    @FunctionalInterface
    private interface BooleanSupplier {
        boolean getAsBoolean() throws Exception;
    }
}
