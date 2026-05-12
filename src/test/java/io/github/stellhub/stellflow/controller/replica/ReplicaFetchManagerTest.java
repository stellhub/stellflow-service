package io.github.stellhub.stellflow.controller.replica;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.stellhub.stellflow.controller.control.ControlPlaneGrpcConfig;
import io.github.stellhub.stellflow.controller.control.ControllerBrokerControlClient;
import io.github.stellhub.stellflow.controller.control.ControllerBrokerControlServer;
import io.github.stellhub.stellflow.controller.control.ControllerMetadataStateMachine;
import io.github.stellhub.stellflow.controller.control.ControllerMetadataDecisionService;
import io.github.stellhub.stellflow.controller.control.PartitionControlResultRegistry;
import io.github.stellhub.stellflow.controller.quorum.ControllerQuorumConfig;
import io.github.stellhub.stellflow.network.protocol.ProtocolCodecRegistry;
import io.github.stellhub.stellflow.network.transport.NettyTransportConfig;
import io.github.stellhub.stellflow.network.transport.SocketServer;
import io.github.stellhub.stellflow.observability.metrics.MetricsHttpConfig;
import io.github.stellhub.stellflow.observability.metrics.PrometheusMetricsHttpServer;
import io.github.stellhub.stellflow.observability.metrics.ReplicaFetchMetrics;
import io.github.stellhub.stellflow.server.api.BrokerApis;
import io.github.stellhub.stellflow.server.api.InMemoryRequestChannel;
import io.github.stellhub.stellflow.server.api.RequestChannel;
import io.github.stellhub.stellflow.server.api.RequestDispatcher;
import io.github.stellhub.stellflow.server.api.ResponseResponder;
import io.github.stellhub.stellflow.storage.log.LogManager;
import io.github.stellhub.stellflow.storage.log.LogStorageConfig;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * ReplicaFetchManager 测试。
 */
class ReplicaFetchManagerTest {

    @TempDir private Path tempDir;

    /**
     * 验证后台拉取循环可通过网络同步数据并暴露指标端口。
     */
    @Test
    void shouldReplicateFromLeaderOverNetworkAndExposeMetrics() throws Exception {
        int leaderPort = findFreePort();
        int metricsPort = findFreePort();
        Path leaderLogDir = tempDir.resolve("leader");
        Path followerLogDir = tempDir.resolve("follower");

        RequestChannel requestChannel = new InMemoryRequestChannel();
        BrokerApis leaderBrokerApis =
                BrokerApis.defaultBrokerApis("127.0.0.1", leaderPort, leaderLogDir);
        RequestDispatcher dispatcher = new RequestDispatcher(requestChannel, leaderBrokerApis, 2);
        ResponseResponder responder = new ResponseResponder(requestChannel);
        SocketServer socketServer =
                new SocketServer(
                        NettyTransportConfig.builder().host("127.0.0.1").port(leaderPort).build(),
                        ProtocolCodecRegistry.defaultRegistry(),
                        requestChannel);

        LogStorageConfig followerConfig =
                LogStorageConfig.builder()
                        .rootDir(followerLogDir)
                        .segmentBytes(1024)
                        .indexIntervalBytes(1)
                        .retentionSegments(8)
                        .retentionMs(7L * 24 * 60 * 60 * 1000)
                        .retentionBytes(1024 * 1024)
                        .build();
        try (LogManager followerLogManager = new LogManager(followerConfig)) {
            ReplicaFetchMetrics metrics = new ReplicaFetchMetrics();
            ReplicaFetchManager manager =
                    new ReplicaFetchManager(
                            ReplicaFetchConfig.builder()
                                    .enabled(true)
                                    .followerBrokerId(1)
                                    .pollIntervalMs(100)
                                    .socketTimeoutMs(3000)
                                    .connectTimeoutMs(3000)
                                    .maxBytes(4096)
                                    .maxWaitMs(200)
                                    .minBytes(1)
                                    .build(),
                            followerLogManager,
                            metrics);
            manager.addAssignment(
                    new ReplicaFetchAssignment("replica-loop", 0, "127.0.0.1", leaderPort, 0));
            PrometheusMetricsHttpServer metricsServer =
                    new PrometheusMetricsHttpServer(
                            MetricsHttpConfig.builder()
                                    .enabled(true)
                                    .host("127.0.0.1")
                                    .port(metricsPort)
                                    .path("/metrics")
                                    .build(),
                            metrics);
            try {
                dispatcher.start();
                responder.start();
                socketServer.start();

                leaderBrokerApis
                        .logManager()
                        .updateReplicaTopology(
                                "replica-loop", 0, 0, List.of(0, 1), List.of(0, 1));
                followerLogManager.updateReplicaTopology(
                        "replica-loop", 0, 0, List.of(0, 1), List.of(0, 1));

                leaderBrokerApis
                        .logManager()
                        .append("replica-loop", 0, "a".getBytes(StandardCharsets.UTF_8));
                leaderBrokerApis
                        .logManager()
                        .append("replica-loop", 0, "b".getBytes(StandardCharsets.UTF_8));

                metricsServer.start();
                manager.start();

                waitUntil(
                        () ->
                                "ab"
                                        .equals(
                                                new String(
                                                        followerLogManager
                                                                .read("replica-loop", 0, 0, 4096)
                                                                .records(),
                                                        StandardCharsets.UTF_8)),
                        5000);
                waitUntil(
                        () -> leaderBrokerApis.logManager().highWatermark("replica-loop", 0) == 2L,
                        5000);

                assertEquals(
                        "ab",
                        new String(
                                followerLogManager.read("replica-loop", 0, 0, 4096).records(),
                                StandardCharsets.UTF_8));
                assertEquals(2L, leaderBrokerApis.logManager().highWatermark("replica-loop", 0));

                HttpClient client = HttpClient.newHttpClient();
                HttpResponse<String> response =
                        client.send(
                                HttpRequest.newBuilder(
                                                URI.create("http://127.0.0.1:" + metricsPort + "/metrics"))
                                        .timeout(Duration.ofSeconds(3))
                                        .GET()
                                        .build(),
                                HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
                assertEquals(200, response.statusCode());
                assertTrue(
                        response.body().contains("stellflow_replica_fetch_requests_total"));
                assertTrue(response.body().contains("topic=\"replica-loop\""));
            } finally {
                metricsServer.close();
                manager.close();
                socketServer.close();
                responder.close();
                dispatcher.close();
                leaderBrokerApis.close();
            }
        }
    }

    /**
     * 验证同一 leader 的多分区抓取可复用单连接并通过 correlation pipeline 并发回包。
     */
    @Test
    void shouldReuseSingleConnectionForMultiplePartitionsOnSameLeader() throws Exception {
        int leaderPort = findFreePort();
        Path leaderLogDir = tempDir.resolve("leader-shared");
        Path followerLogDir = tempDir.resolve("follower-shared");

        RequestChannel requestChannel = new InMemoryRequestChannel();
        BrokerApis leaderBrokerApis =
                BrokerApis.defaultBrokerApis("127.0.0.1", leaderPort, leaderLogDir);
        RequestDispatcher dispatcher = new RequestDispatcher(requestChannel, leaderBrokerApis, 4);
        ResponseResponder responder = new ResponseResponder(requestChannel);
        SocketServer socketServer =
                new SocketServer(
                        NettyTransportConfig.builder().host("127.0.0.1").port(leaderPort).build(),
                        ProtocolCodecRegistry.defaultRegistry(),
                        requestChannel);

        LogStorageConfig followerConfig =
                LogStorageConfig.builder()
                        .rootDir(followerLogDir)
                        .segmentBytes(1024)
                        .indexIntervalBytes(1)
                        .retentionSegments(8)
                        .retentionMs(7L * 24 * 60 * 60 * 1000)
                        .retentionBytes(1024 * 1024)
                        .build();
        try (LogManager followerLogManager = new LogManager(followerConfig)) {
            ReplicaFetchMetrics metrics = new ReplicaFetchMetrics();
            ReplicaFetchManager manager =
                    new ReplicaFetchManager(
                            ReplicaFetchConfig.builder()
                                    .enabled(true)
                                    .followerBrokerId(1)
                                    .pollIntervalMs(100)
                                    .socketTimeoutMs(3000)
                                    .connectTimeoutMs(3000)
                                    .maxBytes(4096)
                                    .maxWaitMs(200)
                                    .minBytes(1)
                                    .pipelineRoundsPerPoll(4)
                                    .build(),
                            followerLogManager,
                            metrics);
            manager.addAssignment(
                    new ReplicaFetchAssignment("shared-topic", 0, "127.0.0.1", leaderPort, 0));
            manager.addAssignment(
                    new ReplicaFetchAssignment("shared-topic", 1, "127.0.0.1", leaderPort, 0));
            try {
                dispatcher.start();
                responder.start();
                socketServer.start();

                leaderBrokerApis
                        .logManager()
                        .updateReplicaTopology(
                                "shared-topic", 0, 0, List.of(0, 1), List.of(0, 1));
                leaderBrokerApis
                        .logManager()
                        .updateReplicaTopology(
                                "shared-topic", 1, 0, List.of(0, 1), List.of(0, 1));
                followerLogManager.updateReplicaTopology(
                        "shared-topic", 0, 0, List.of(0, 1), List.of(0, 1));
                followerLogManager.updateReplicaTopology(
                        "shared-topic", 1, 0, List.of(0, 1), List.of(0, 1));

                leaderBrokerApis
                        .logManager()
                        .append("shared-topic", 0, "a0".getBytes(StandardCharsets.UTF_8));
                leaderBrokerApis
                        .logManager()
                        .append("shared-topic", 1, "b1".getBytes(StandardCharsets.UTF_8));

                manager.start();

                waitUntil(
                        () ->
                                "a0"
                                        .equals(
                                                new String(
                                                        followerLogManager
                                                                .read("shared-topic", 0, 0, 4096)
                                                                .records(),
                                                        StandardCharsets.UTF_8))
                                        && "b1"
                                                .equals(
                                                        new String(
                                                                followerLogManager
                                                                        .read(
                                                                                "shared-topic",
                                                                                1,
                                                                                0,
                                                                                4096)
                                                                        .records(),
                                                                StandardCharsets.UTF_8)),
                        5000);

                assertEquals(1, manager.activeConnectionCount());
            } finally {
                manager.close();
                socketServer.close();
                responder.close();
                dispatcher.close();
                leaderBrokerApis.close();
            }
        }
    }

    /**
     * 验证 gRPC 控制面可动态下发 assignments 并触发 follower 拉取。
     */
    @Test
    void shouldReceiveDynamicAssignmentsFromGrpcControlPlane() throws Exception {
        int leaderPort = findFreePort();
        int controllerPort = findFreePort();
        int quorumPort = findFreePort();
        Path leaderLogDir = tempDir.resolve("leader-grpc");
        Path followerLogDir = tempDir.resolve("follower-grpc");

        RequestChannel requestChannel = new InMemoryRequestChannel();
        BrokerApis leaderBrokerApis =
                BrokerApis.defaultBrokerApis("127.0.0.1", leaderPort, leaderLogDir);
        RequestDispatcher dispatcher = new RequestDispatcher(requestChannel, leaderBrokerApis, 2);
        ResponseResponder responder = new ResponseResponder(requestChannel);
        SocketServer socketServer =
                new SocketServer(
                        NettyTransportConfig.builder().host("127.0.0.1").port(leaderPort).build(),
                        ProtocolCodecRegistry.defaultRegistry(),
                        requestChannel);

        LogStorageConfig followerConfig =
                LogStorageConfig.builder()
                        .rootDir(followerLogDir)
                        .segmentBytes(1024)
                        .indexIntervalBytes(1)
                        .retentionSegments(8)
                        .retentionMs(7L * 24 * 60 * 60 * 1000)
                        .retentionBytes(1024 * 1024)
                        .build();
        try (LogManager followerLogManager = new LogManager(followerConfig)) {
            ReplicaFetchMetrics metrics = new ReplicaFetchMetrics();
            ReplicaFetchManager manager =
                    new ReplicaFetchManager(
                            ReplicaFetchConfig.builder()
                                    .enabled(true)
                                    .followerBrokerId(2)
                                    .pollIntervalMs(100)
                                    .socketTimeoutMs(3000)
                                    .connectTimeoutMs(3000)
                                    .maxBytes(4096)
                                    .maxWaitMs(200)
                                    .minBytes(1)
                                    .pipelineRoundsPerPoll(4)
                                    .build(),
                            followerLogManager,
                            metrics);
            ControllerBrokerControlServer server =
                    new ControllerBrokerControlServer(
                            ControlPlaneGrpcConfig.builder()
                                    .serverEnabled(true)
                                    .serverHost("127.0.0.1")
                                    .serverPort(controllerPort)
                                    .clusterId("test-cluster")
                                    .build(),
                            new io.github.stellhub.stellflow.controller.control.ControllerAssignmentRegistry(),
                            new io.github.stellhub.stellflow.controller.control.ControllerPartitionControlRegistry(),
                            new PartitionControlResultRegistry(),
                            ControllerQuorumConfig.builder()
                                    .enabled(true)
                                    .selfId("c1")
                                    .groupId("22222222-2222-2222-2222-222222222222")
                                    .endpoint("grpc://127.0.0.1:" + quorumPort)
                                    .storageDir(tempDir.resolve("controller-quorum"))
                                    .peers("c1@grpc://127.0.0.1:" + quorumPort)
                                    .requestTimeoutMs(3000)
                                    .build(),
                            leaderBrokerApis.controllerReplicaCoordinator());
            ControllerBrokerControlClient client =
                    new ControllerBrokerControlClient(
                            ControlPlaneGrpcConfig.builder()
                                    .clientEnabled(true)
                                    .controllerHost("127.0.0.1")
                                    .controllerPort(controllerPort)
                                    .brokerId(2)
                                    .advertisedHost("127.0.0.1")
                                    .advertisedPort(leaderPort)
                                    .watchReconnectBackoffMs(200)
                                    .build(),
                            manager,
                            new ControllerReplicaCoordinator(followerLogManager));
            try {
                dispatcher.start();
                responder.start();
                socketServer.start();
                server.start();

                leaderBrokerApis
                        .logManager()
                        .updateReplicaTopology(
                                "replica-grpc", 0, 0, List.of(0, 2), List.of(0, 2));
                followerLogManager.updateReplicaTopology(
                        "replica-grpc", 0, 0, List.of(0, 2), List.of(0, 2));

                manager.start();
                client.start();

                PartitionControlResultRegistry resultRegistry = server.partitionControlResultRegistry();
                ControllerMetadataDecisionService decisionService = server.metadataDecisionService();
                decisionService.registerBroker(
                        0,
                        "stellflow://127.0.0.1:" + leaderPort,
                        "127.0.0.1",
                        leaderPort,
                        System.currentTimeMillis());
                waitUntil(() -> server.metadataStateMachine().brokers().size() == 2, 5000);
                decisionService.createTopic("replica-grpc", 1, 2);

                leaderBrokerApis
                        .logManager()
                        .append("replica-grpc", 0, "x".getBytes(StandardCharsets.UTF_8));
                leaderBrokerApis
                        .logManager()
                        .append("replica-grpc", 0, "y".getBytes(StandardCharsets.UTF_8));

                waitUntil(
                        () ->
                                "xy"
                                        .equals(
                                                new String(
                                                        followerLogManager
                                                                .read("replica-grpc", 0, 0, 4096)
                                                                .records(),
                                                        StandardCharsets.UTF_8)),
                        5000);

                assertEquals(
                        "xy",
                        new String(
                                followerLogManager.read("replica-grpc", 0, 0, 4096).records(),
                                StandardCharsets.UTF_8));

                decisionService.changeLeaderAndIsr(
                        "replica-grpc",
                        0,
                        2,
                        3,
                        List.of(0, 2),
                        null,
                        1L);

                waitUntil(
                        () -> !resultRegistry.latestReport(2).isEmpty(),
                        5000);
                waitUntil(
                        () -> followerLogManager.leaderEpoch("replica-grpc", 0) == 3,
                        5000);
                waitUntil(
                        () ->
                                "x"
                                        .equals(
                                                new String(
                                                        followerLogManager
                                                                .read("replica-grpc", 0, 0, 4096)
                                                                .records(),
                                                        StandardCharsets.UTF_8)),
                        5000);
                assertTrue(resultRegistry.latestReport(2).get(0).getSuccess());
                assertEquals(3, followerLogManager.leaderEpoch("replica-grpc", 0));
                assertEquals(
                        "x",
                        new String(
                                followerLogManager.read("replica-grpc", 0, 0, 4096).records(),
                                StandardCharsets.UTF_8));
            } finally {
                client.close();
                server.close();
                manager.close();
                socketServer.close();
                responder.close();
                dispatcher.close();
                leaderBrokerApis.close();
            }
        }
    }

    /**
     * 验证 controller 删除 topic 后，会通过 partition control delete 命令联动清理 broker 本地存储。
     */
    @Test
    void shouldDeleteLocalPartitionDataWhenControllerDeletesTopic() throws Exception {
        int leaderPort = findFreePort();
        int controllerPort = findFreePort();
        int quorumPort = findFreePort();
        Path leaderLogDir = tempDir.resolve("leader-delete");
        Path followerLogDir = tempDir.resolve("follower-delete");

        RequestChannel requestChannel = new InMemoryRequestChannel();
        BrokerApis leaderBrokerApis =
                BrokerApis.defaultBrokerApis("127.0.0.1", leaderPort, leaderLogDir);
        RequestDispatcher dispatcher = new RequestDispatcher(requestChannel, leaderBrokerApis, 2);
        ResponseResponder responder = new ResponseResponder(requestChannel);
        SocketServer socketServer =
                new SocketServer(
                        NettyTransportConfig.builder().host("127.0.0.1").port(leaderPort).build(),
                        ProtocolCodecRegistry.defaultRegistry(),
                        requestChannel);

        LogStorageConfig followerConfig =
                LogStorageConfig.builder()
                        .rootDir(followerLogDir)
                        .segmentBytes(1024)
                        .indexIntervalBytes(1)
                        .retentionSegments(8)
                        .retentionMs(7L * 24 * 60 * 60 * 1000)
                        .retentionBytes(1024 * 1024)
                        .build();
        try (LogManager followerLogManager = new LogManager(followerConfig)) {
            ReplicaFetchManager leaderManager =
                    new ReplicaFetchManager(
                            ReplicaFetchConfig.builder().enabled(false).followerBrokerId(0).build(),
                            leaderBrokerApis.logManager(),
                            new ReplicaFetchMetrics());
            ReplicaFetchManager followerManager =
                    new ReplicaFetchManager(
                            ReplicaFetchConfig.builder().enabled(false).followerBrokerId(2).build(),
                            followerLogManager,
                            new ReplicaFetchMetrics());
            ControllerBrokerControlServer server =
                    new ControllerBrokerControlServer(
                            ControlPlaneGrpcConfig.builder()
                                    .serverEnabled(true)
                                    .serverHost("127.0.0.1")
                                    .serverPort(controllerPort)
                                    .clusterId("delete-cluster")
                                    .build(),
                            new io.github.stellhub.stellflow.controller.control.ControllerAssignmentRegistry(),
                            new io.github.stellhub.stellflow.controller.control.ControllerPartitionControlRegistry(),
                            new PartitionControlResultRegistry(),
                            ControllerQuorumConfig.builder()
                                    .enabled(true)
                                    .selfId("c1")
                                    .groupId("44444444-4444-4444-4444-444444444444")
                                    .endpoint("grpc://127.0.0.1:" + quorumPort)
                                    .storageDir(tempDir.resolve("delete-controller-quorum"))
                                    .peers("c1@grpc://127.0.0.1:" + quorumPort)
                                    .requestTimeoutMs(3000)
                                    .build(),
                            leaderBrokerApis.controllerReplicaCoordinator());
            ControllerBrokerControlClient leaderClient =
                    new ControllerBrokerControlClient(
                            ControlPlaneGrpcConfig.builder()
                                    .clientEnabled(true)
                                    .controllerHost("127.0.0.1")
                                    .controllerPort(controllerPort)
                                    .brokerId(0)
                                    .advertisedHost("127.0.0.1")
                                    .advertisedPort(leaderPort)
                                    .watchReconnectBackoffMs(200)
                                    .registrationIntervalMs(200)
                                    .build(),
                            leaderManager,
                            leaderBrokerApis.controllerReplicaCoordinator());
            ControllerBrokerControlClient followerClient =
                    new ControllerBrokerControlClient(
                            ControlPlaneGrpcConfig.builder()
                                    .clientEnabled(true)
                                    .controllerHost("127.0.0.1")
                                    .controllerPort(controllerPort)
                                    .brokerId(2)
                                    .advertisedHost("127.0.0.1")
                                    .advertisedPort(leaderPort + 1)
                                    .watchReconnectBackoffMs(200)
                                    .registrationIntervalMs(200)
                                    .build(),
                            followerManager,
                            new ControllerReplicaCoordinator(followerLogManager));
            try {
                dispatcher.start();
                responder.start();
                socketServer.start();
                server.start();

                leaderClient.start();
                followerClient.start();

                ControllerMetadataDecisionService decisionService = server.metadataDecisionService();
                waitUntil(() -> server.metadataStateMachine().brokers().size() == 2, 5000);

                decisionService.createTopic("delete-topic", 1, 2);

                waitUntil(
                        () -> leaderBrokerApis.logManager().containsPartition("delete-topic", 0),
                        5000);
                waitUntil(() -> followerLogManager.containsPartition("delete-topic", 0), 5000);

                leaderBrokerApis
                        .logManager()
                        .append("delete-topic", 0, "payload".getBytes(StandardCharsets.UTF_8));

                decisionService.deleteTopic("delete-topic");

                waitUntil(
                        () -> !leaderBrokerApis.logManager().containsPartition("delete-topic", 0),
                        5000);
                waitUntil(() -> !followerLogManager.containsPartition("delete-topic", 0), 5000);
                waitUntil(
                        () -> server.partitionControlResultRegistry().latestReport(0).stream()
                                        .anyMatch(
                                                result ->
                                                        result.getSuccess()
                                                                && result.getDeletePartition())
                                && server.partitionControlResultRegistry().latestReport(2).stream()
                                        .anyMatch(
                                                result ->
                                                        result.getSuccess()
                                                                && result.getDeletePartition()),
                        5000);

                assertFalse(server.metadataStateMachine().topic("delete-topic").isPresent());
                assertFalse(leaderBrokerApis.logManager().containsPartition("delete-topic", 0));
                assertFalse(followerLogManager.containsPartition("delete-topic", 0));
            } finally {
                followerClient.close();
                leaderClient.close();
                server.close();
                followerManager.close();
                leaderManager.close();
                socketServer.close();
                responder.close();
                dispatcher.close();
                leaderBrokerApis.close();
            }
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
        assertTrue(supplier.getAsBoolean());
    }

    @FunctionalInterface
    private interface BooleanSupplier {
        boolean getAsBoolean() throws Exception;
    }
}
