package io.github.stellhub.stellflow.controller.quorum;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import io.github.stellhub.stellflow.controller.control.ControllerAssignmentRegistry;
import io.github.stellhub.stellflow.controller.control.ControllerMetadataDecisionService;
import io.github.stellhub.stellflow.controller.control.ControllerMetadataStateMachine;
import io.github.stellhub.stellflow.controller.control.ControllerPartitionControlRegistry;
import io.github.stellhub.stellflow.controller.control.PartitionControlResultRegistry;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.ArrayList;
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
            ControllerMetadataDecisionService decisionService =
                    new ControllerMetadataDecisionService(quorumManager, metadataStateMachine);
            decisionService.registerBroker(
                    1, "stellflow://broker-a.example.com:9092", "broker-a.example.com", 9092, 10L);
            decisionService.registerBroker(
                    2, "stellflow://broker-b.example.com:9092", "broker-b.example.com", 9093, 11L);
            decisionService.createTopic("orders", 1, 2);

            waitUntil(() -> metadataStateMachine.brokers().size() == 2, 5000);
            waitUntil(() -> metadataStateMachine.partitions().size() == 1, 5000);
            waitUntil(() -> metadataStateMachine.topics().size() == 1, 5000);

            assertEquals(
                    "broker-a.example.com",
                    metadataStateMachine.broker(1).orElseThrow().advertisedHost());
            assertEquals(1, metadataStateMachine.partitions().get(0).leaderEpoch());
            assertEquals("orders", metadataStateMachine.topics().get(0).topic());
            assertFalse(metadataStateMachine.partitions().isEmpty());
        }
    }

    /**
     * 验证三节点 quorum 可提交并复制元数据记录。
     */
    @Test
    void shouldApplyCommittedMetadataRecordsThroughThreeNodeQuorum() throws Exception {
        int p1 = findFreePort();
        int p2 = findFreePort();
        int p3 = findFreePort();
        String peers =
                "c1@grpc://127.0.0.1:"
                        + p1
                        + ",c2@grpc://127.0.0.1:"
                        + p2
                        + ",c3@grpc://127.0.0.1:"
                        + p3;
        List<ControllerMetadataStateMachine> stateMachines = new ArrayList<>();
        List<ControllerQuorumManager> managers = new ArrayList<>();
        try {
            stateMachines.add(stateMachine());
            stateMachines.add(stateMachine());
            stateMachines.add(stateMachine());
            managers.add(manager("c1", p1, peers, tempDir.resolve("q1"), stateMachines.get(0)));
            managers.add(manager("c2", p2, peers, tempDir.resolve("q2"), stateMachines.get(1)));
            managers.add(manager("c3", p3, peers, tempDir.resolve("q3"), stateMachines.get(2)));
            for (ControllerQuorumManager manager : managers) {
                manager.start();
            }

            ControllerMetadataDecisionService decisionService =
                    new ControllerMetadataDecisionService(managers.get(0), stateMachines.get(0));
            decisionService.registerBroker(
                    1, "stellflow://broker-a.example.com:9092", "broker-a.example.com", 9092, 10L);
            decisionService.createTopic("orders-three-node", 1, 1);

            for (ControllerMetadataStateMachine stateMachine : stateMachines) {
                waitUntil(() -> stateMachine.brokers().size() == 1, 8000);
                waitUntil(() -> stateMachine.topics().size() == 1, 8000);
                assertEquals("orders-three-node", stateMachine.topics().get(0).topic());
            }
        } finally {
            for (ControllerQuorumManager manager : managers) {
                manager.close();
            }
        }
    }

    private static ControllerMetadataStateMachine stateMachine() {
        return new ControllerMetadataStateMachine(
                new ControllerAssignmentRegistry(),
                new ControllerPartitionControlRegistry(),
                new PartitionControlResultRegistry());
    }

    private static ControllerQuorumManager manager(
            String selfId,
            int port,
            String peers,
            Path storageDir,
            ControllerMetadataStateMachine stateMachine) {
        return new ControllerQuorumManager(
                ControllerQuorumConfig.builder()
                        .enabled(true)
                        .selfId(selfId)
                        .groupId("44444444-4444-4444-4444-444444444444")
                        .endpoint("grpc://127.0.0.1:" + port)
                        .storageDir(storageDir)
                        .peers(peers)
                        .requestTimeoutMs(8000)
                        .build(),
                stateMachine);
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
