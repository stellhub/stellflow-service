package io.github.stellhub.stellflow.tools.validation;

import io.github.stellhub.stellflow.controller.control.ControlPlaneGrpcConfig;
import io.github.stellhub.stellflow.controller.control.ControllerAssignmentRegistry;
import io.github.stellhub.stellflow.controller.control.ControllerBrokerControlClient;
import io.github.stellhub.stellflow.controller.control.ControllerBrokerControlServer;
import io.github.stellhub.stellflow.controller.control.ControllerMetadataDecisionService;
import io.github.stellhub.stellflow.controller.control.ControllerPartitionMetadata;
import io.github.stellhub.stellflow.controller.control.ControllerPartitionControlRegistry;
import io.github.stellhub.stellflow.controller.control.PartitionControlResultRegistry;
import io.github.stellhub.stellflow.controller.quorum.ControllerQuorumConfig;
import io.github.stellhub.stellflow.controller.replica.ControllerReplicaCoordinator;
import io.github.stellhub.stellflow.controller.replica.ReplicaFetchConfig;
import io.github.stellhub.stellflow.controller.replica.ReplicaFetchManager;
import io.github.stellhub.stellflow.metadata.PartitionRole;
import io.github.stellhub.stellflow.network.protocol.ApiKey;
import io.github.stellhub.stellflow.network.protocol.ErrorCode;
import io.github.stellhub.stellflow.network.protocol.ProtocolCodecRegistry;
import io.github.stellhub.stellflow.network.transport.NettyTransportConfig;
import io.github.stellhub.stellflow.network.transport.SocketServer;
import io.github.stellhub.stellflow.observability.metrics.ReplicaFetchMetrics;
import io.github.stellhub.stellflow.server.api.BrokerApis;
import io.github.stellhub.stellflow.server.api.InMemoryRequestChannel;
import io.github.stellhub.stellflow.server.api.RequestChannel;
import io.github.stellhub.stellflow.server.api.RequestDispatcher;
import io.github.stellhub.stellflow.server.api.ResponseResponder;
import io.github.stellhub.stellflow.storage.log.LogManager;
import io.github.stellhub.stellflow.storage.log.LogStorageConfig;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * P0 本地三 Broker 集群验证器。
 */
final class ClusterFailoverValidator {

    private static final String TOPIC = "p0-cluster-topic";

    /**
     * 执行 P0 三节点闭环验证。
     */
    ValidationResult run() {
        long startMs = System.currentTimeMillis();
        Path rootDir = null;
        P0ControllerNode controller = null;
        List<P0BrokerNode> brokers = new ArrayList<>();
        TemporarySystemProperties properties = new TemporarySystemProperties();
        try {
            rootDir = Files.createTempDirectory("stellflow-p0-cluster-");
            properties.set("stellflow.controlPlane.reconcile.intervalMs", "100");
            properties.set("stellflow.controlPlane.reconcile.brokerHeartbeatTimeoutMs", "1500");
            properties.set("stellflow.controlPlane.reconcile.maxReplicaLagMessages", "0");
            properties.set("stellflow.controlPlane.reconcile.uncleanLeaderElectionEnabled", "false");

            int controllerPort = findFreePort();
            int quorumPort = findFreePort();
            controller = new P0ControllerNode(rootDir.resolve("controller"), controllerPort, quorumPort);
            controller.start();
            P0ControllerNode runningController = controller;

            for (int brokerId = 1; brokerId <= 3; brokerId++) {
                P0BrokerNode broker =
                        new P0BrokerNode(
                                brokerId,
                                findFreePort(),
                                controllerPort,
                                rootDir.resolve("broker-" + brokerId));
                broker.start();
                brokers.add(broker);
            }

            Map<Integer, P0BrokerNode> brokersById = brokersById(brokers);
            waitUntil(
                    () -> runningController.server.metadataStateMachine().brokers().size() == 3,
                    10_000);

            ControllerMetadataDecisionService decisionService =
                    runningController.server.metadataDecisionService();
            decisionService.createTopic(TOPIC, 1, 3);
            waitUntil(() -> allContainPartition(brokers, TOPIC, 0), 10_000);

            int firstLeaderId = leaderId(runningController, TOPIC, 0);
            ProduceResult firstProduce =
                    produceWithLeaderRetry(
                            runningController,
                            brokersById,
                            brokers,
                            "p0-first".getBytes(StandardCharsets.UTF_8),
                            1,
                            10_000);
            requireNoError(firstProduce.errorCode(), "first produce");
            waitUntil(() -> allReachLogEndOffset(brokers, TOPIC, 0, 1), 10_000);

            P0BrokerNode firstLeader = brokersById.get(firstLeaderId);
            firstLeader.closeForFailure();
            waitUntil(
                    () ->
                            runningController.server
                                    .metadataStateMachine()
                                    .broker(firstLeaderId)
                                    .map(broker -> broker.fenced())
                                    .orElse(false),
                    10_000);
            waitUntil(() -> leaderId(runningController, TOPIC, 0) != firstLeaderId, 10_000);

            ProduceResult secondProduce =
                    produceWithLeaderRetry(
                            runningController,
                            brokersById,
                            brokers,
                            "p0-second".getBytes(StandardCharsets.UTF_8),
                            2,
                            10_000);
            requireNoError(secondProduce.errorCode(), "second produce after failover");
            waitUntil(
                    () -> liveBrokersReachLogEndOffset(brokers, TOPIC, 0, 2),
                    10_000);

            int secondLeaderId = leaderId(runningController, TOPIC, 0);
            P0BrokerNode secondLeader = brokersById.get(secondLeaderId);
            byte[] fetched = ProtocolClient.fetch("127.0.0.1", secondLeader.port, TOPIC, 0, 0, 4096, 3);
            String payload = new String(fetched, StandardCharsets.UTF_8);
            if (!"p0-firstp0-second".equals(payload)) {
                throw new IllegalStateException("Unexpected fetched payload: " + payload);
            }

            String message =
                    "p0 cluster closed loop passed: brokers=3 topic="
                            + TOPIC
                            + " leaderFailover="
                            + firstLeaderId
                            + "->"
                            + secondLeaderId;
            return new ValidationResult(
                    ValidationScenario.P0_CLUSTER,
                    true,
                    System.currentTimeMillis() - startMs,
                    message);
        } catch (Exception exception) {
            return new ValidationResult(
                    ValidationScenario.P0_CLUSTER,
                    false,
                    System.currentTimeMillis() - startMs,
                    exception.getMessage());
        } finally {
            for (P0BrokerNode broker : brokers) {
                broker.close();
            }
            if (controller != null) {
                controller.close();
            }
            properties.close();
            deleteDirectory(rootDir);
        }
    }

    private static Map<Integer, P0BrokerNode> brokersById(List<P0BrokerNode> brokers) {
        Map<Integer, P0BrokerNode> values = new HashMap<>();
        for (P0BrokerNode broker : brokers) {
            values.put(broker.brokerId, broker);
        }
        return values;
    }

    private static boolean allContainPartition(List<P0BrokerNode> brokers, String topic, int partition) {
        return brokers.stream()
                .allMatch(broker -> broker.brokerApis.logManager().containsPartition(topic, partition));
    }

    private static boolean allReachLogEndOffset(
            List<P0BrokerNode> brokers, String topic, int partition, long offset) {
        return brokers.stream()
                .allMatch(broker -> broker.brokerApis.logManager().logEndOffset(topic, partition) >= offset);
    }

    private static boolean liveBrokersReachLogEndOffset(
            List<P0BrokerNode> brokers, String topic, int partition, long offset) {
        return brokers.stream()
                .filter(broker -> !broker.closed)
                .allMatch(broker -> broker.brokerApis.logManager().logEndOffset(topic, partition) >= offset);
    }

    private static boolean liveBrokersObserveLeader(
            List<P0BrokerNode> brokers, String topic, int partition, int leaderId) {
        return brokers.stream()
                .filter(broker -> !broker.closed)
                .allMatch(
                        broker ->
                                broker.brokerApis
                                        .metadataCache()
                                        .partition(topic, partition)
                                        .map(
                                                metadata ->
                                                        metadata.leaderId() == leaderId
                                                                && (broker.brokerId != leaderId
                                                                        || metadata.localRole()
                                                                                == PartitionRole.LEADER))
                                        .orElse(false));
    }

    private static int leaderId(P0ControllerNode controller, String topic, int partition) {
        Optional<ControllerPartitionMetadata> metadata =
                controller.server.metadataStateMachine().partition(topic, partition);
        return metadata.orElseThrow(() -> new IllegalStateException("Partition metadata not found"))
                .leaderId();
    }

    private static void requireNoError(short errorCode, String operation) {
        if (errorCode != ErrorCode.NONE.code()) {
            throw new IllegalStateException(operation + " failed with errorCode=" + errorCode);
        }
    }

    private static ProduceResult produceWithLeaderRetry(
            P0ControllerNode controller,
            Map<Integer, P0BrokerNode> brokersById,
            List<P0BrokerNode> brokers,
            byte[] records,
            int correlationIdBase,
            long timeoutMs)
            throws Exception {
        long deadlineMs = System.currentTimeMillis() + timeoutMs;
        short lastError = ErrorCode.LEADER_NOT_AVAILABLE.code();
        int attempt = 0;
        while (System.currentTimeMillis() < deadlineMs) {
            int currentLeaderId = leaderId(controller, TOPIC, 0);
            P0BrokerNode leader = brokersById.get(currentLeaderId);
            if (leader == null || leader.closed) {
                Thread.sleep(50);
                continue;
            }
            if (!liveBrokersObserveLeader(brokers, TOPIC, 0, currentLeaderId)) {
                Thread.sleep(50);
                continue;
            }
            ProduceResult result =
                    ProtocolClient.produce(
                            "127.0.0.1",
                            leader.port,
                            TOPIC,
                            0,
                            records,
                            20_000,
                            correlationIdBase + attempt);
            if (result.errorCode() == ErrorCode.NONE.code()) {
                return result;
            }
            lastError = result.errorCode();
            if (lastError != ErrorCode.NOT_LEADER_OR_FOLLOWER.code()
                    && lastError != ErrorCode.LEADER_NOT_AVAILABLE.code()) {
                return result;
            }
            attempt++;
            Thread.sleep(50);
        }
        return new ProduceResult(lastError, -1);
    }

    private static int findFreePort() throws IOException {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            return serverSocket.getLocalPort();
        }
    }

    private static void waitUntil(BooleanSupplier supplier, long timeoutMs) throws Exception {
        long deadlineMs = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadlineMs) {
            if (supplier.getAsBoolean()) {
                return;
            }
            Thread.sleep(50);
        }
        if (!supplier.getAsBoolean()) {
            throw new IllegalStateException("Condition was not satisfied within " + timeoutMs + "ms");
        }
    }

    private static void deleteDirectory(Path directory) {
        if (directory == null || !Files.exists(directory)) {
            return;
        }
        try (Stream<Path> stream = Files.walk(directory)) {
            stream.sorted(Comparator.reverseOrder())
                    .forEach(
                            path -> {
                                try {
                                    Files.deleteIfExists(path);
                                } catch (IOException exception) {
                                    throw new IllegalStateException(
                                            "Failed to delete temp path " + path, exception);
                                }
                            });
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to delete temp directory " + directory, exception);
        }
    }

    @FunctionalInterface
    private interface BooleanSupplier {
        boolean getAsBoolean() throws Exception;
    }

    private static final class P0ControllerNode implements AutoCloseable {

        private final LogManager progressLogManager;
        private final ControllerBrokerControlServer server;

        private P0ControllerNode(Path rootDir, int controllerPort, int quorumPort) {
            this.progressLogManager =
                    new LogManager(
                            LogStorageConfig.builder()
                                    .rootDir(rootDir.resolve("progress"))
                                    .segmentBytes(1024)
                                    .indexIntervalBytes(1)
                                    .retentionSegments(8)
                                    .retentionMs(7L * 24 * 60 * 60 * 1000)
                                    .retentionBytes(1024 * 1024)
                                    .build());
            this.server =
                    new ControllerBrokerControlServer(
                            ControlPlaneGrpcConfig.builder()
                                    .serverEnabled(true)
                                    .serverHost("127.0.0.1")
                                    .serverPort(controllerPort)
                                    .clusterId("p0-cluster")
                                    .requirePersistentMetadata(true)
                                    .build(),
                            new ControllerAssignmentRegistry(),
                            new ControllerPartitionControlRegistry(),
                            new PartitionControlResultRegistry(),
                            ControllerQuorumConfig.builder()
                                    .enabled(true)
                                    .selfId("c1")
                                    .groupId(UUID.randomUUID().toString())
                                    .endpoint("grpc://127.0.0.1:" + quorumPort)
                                    .storageDir(rootDir.resolve("quorum"))
                                    .peers("c1@grpc://127.0.0.1:" + quorumPort)
                                    .requestTimeoutMs(8000)
                                    .build(),
                            new ControllerReplicaCoordinator(progressLogManager));
        }

        private void start() throws InterruptedException {
            server.start();
        }

        @Override
        public void close() {
            server.close();
            progressLogManager.close();
        }
    }

    private static final class P0BrokerNode implements AutoCloseable {

        private final int brokerId;
        private final int port;
        private final RequestChannel requestChannel;
        private final BrokerApis brokerApis;
        private final RequestDispatcher dispatcher;
        private final ResponseResponder responder;
        private final SocketServer socketServer;
        private final ReplicaFetchManager replicaFetchManager;
        private final ControllerBrokerControlClient controlClient;
        private volatile boolean closed;

        private P0BrokerNode(int brokerId, int port, int controllerPort, Path logDir) {
            this.brokerId = brokerId;
            this.port = port;
            this.requestChannel = new InMemoryRequestChannel();
            this.brokerApis = BrokerApis.defaultBrokerApis(brokerId, "127.0.0.1", port, logDir);
            this.dispatcher = new RequestDispatcher(requestChannel, brokerApis, 4);
            this.responder = new ResponseResponder(requestChannel);
            this.socketServer =
                    new SocketServer(
                            NettyTransportConfig.builder()
                                    .host("127.0.0.1")
                                    .port(port)
                                    .workerThreads(4)
                                    .build(),
                            ProtocolCodecRegistry.defaultRegistry(),
                            requestChannel);
            this.replicaFetchManager =
                    new ReplicaFetchManager(
                            ReplicaFetchConfig.builder()
                                    .enabled(true)
                                    .followerBrokerId(brokerId)
                                    .pollIntervalMs(50)
                                    .socketTimeoutMs(3000)
                                    .connectTimeoutMs(3000)
                                    .maxBytes(4096)
                                    .maxWaitMs(100)
                                    .minBytes(1)
                                    .pipelineRoundsPerPoll(4)
                                    .build(),
                            brokerApis.logManager(),
                            new ReplicaFetchMetrics());
            this.controlClient =
                    new ControllerBrokerControlClient(
                            ControlPlaneGrpcConfig.builder()
                                    .clientEnabled(true)
                                    .controllerHost("127.0.0.1")
                                    .controllerPort(controllerPort)
                                    .brokerId(brokerId)
                                    .advertisedHost("127.0.0.1")
                                    .advertisedPort(port)
                                    .watchReconnectBackoffMs(100)
                                    .registrationIntervalMs(100)
                                    .build(),
                            replicaFetchManager,
                            brokerApis.controllerReplicaCoordinator(),
                            brokerApis.metadataCache());
        }

        private void start() throws InterruptedException {
            dispatcher.start();
            responder.start();
            socketServer.start();
            replicaFetchManager.start();
            controlClient.start();
        }

        private void closeForFailure() {
            close();
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            controlClient.close();
            replicaFetchManager.close();
            socketServer.close();
            responder.close();
            dispatcher.close();
            brokerApis.close();
        }
    }

    private static final class TemporarySystemProperties implements AutoCloseable {

        private final Map<String, String> previousValues = new HashMap<>();

        private void set(String key, String value) {
            previousValues.putIfAbsent(key, System.getProperty(key));
            System.setProperty(key, value);
        }

        @Override
        public void close() {
            for (Map.Entry<String, String> entry : previousValues.entrySet()) {
                if (entry.getValue() == null) {
                    System.clearProperty(entry.getKey());
                } else {
                    System.setProperty(entry.getKey(), entry.getValue());
                }
            }
        }
    }

    private record ProduceResult(short errorCode, long baseOffset) {}

    private static final class ProtocolClient {

        private ProtocolClient() {}

        private static ProduceResult produce(
                String host,
                int port,
                String topic,
                int partition,
                byte[] records,
                int timeoutMs,
                int correlationId)
                throws IOException {
            try (Socket socket = new Socket(host, port)) {
                socket.setSoTimeout(timeoutMs + 3000);
                DataOutputStream output =
                        new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
                DataInputStream input =
                        new DataInputStream(new BufferedInputStream(socket.getInputStream()));
                writeFrame(
                        output,
                        encodeRequestHeader(ApiKey.PRODUCE.code(), correlationId, "p0-producer"),
                        encodeProduceRequestBody(topic, partition, records, timeoutMs));
                readResponseHeader(input, correlationId);
                int topicCount = input.readInt();
                if (topicCount != 1) {
                    throw new IllegalStateException("Unexpected produce topic count " + topicCount);
                }
                readNullableString(input);
                int partitionCount = input.readInt();
                if (partitionCount != 1) {
                    throw new IllegalStateException(
                            "Unexpected produce partition count " + partitionCount);
                }
                input.readInt();
                short errorCode = input.readShort();
                long baseOffset = input.readLong();
                return new ProduceResult(errorCode, baseOffset);
            }
        }

        private static byte[] fetch(
                String host,
                int port,
                String topic,
                int partition,
                long fetchOffset,
                int maxBytes,
                int correlationId)
                throws IOException {
            try (Socket socket = new Socket(host, port)) {
                socket.setSoTimeout(5000);
                DataOutputStream output =
                        new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
                DataInputStream input =
                        new DataInputStream(new BufferedInputStream(socket.getInputStream()));
                writeFrame(
                        output,
                        encodeRequestHeader(ApiKey.FETCH.code(), correlationId, "p0-consumer"),
                        encodeFetchRequestBody(topic, partition, fetchOffset, maxBytes));
                readResponseHeader(input, correlationId);
                input.readInt();
                int topicCount = input.readInt();
                if (topicCount != 1) {
                    throw new IllegalStateException("Unexpected fetch topic count " + topicCount);
                }
                readNullableString(input);
                int partitionCount = input.readInt();
                if (partitionCount != 1) {
                    throw new IllegalStateException(
                            "Unexpected fetch partition count " + partitionCount);
                }
                input.readInt();
                short errorCode = input.readShort();
                if (errorCode != ErrorCode.NONE.code()) {
                    throw new IllegalStateException("Fetch failed with errorCode=" + errorCode);
                }
                input.readLong();
                input.readLong();
                input.readLong();
                int abortedCount = input.readInt();
                for (int index = 0; index < abortedCount; index++) {
                    input.readLong();
                    input.readLong();
                }
                return readBytes(input);
            }
        }

        private static void writeFrame(DataOutputStream output, byte[] header, byte[] body)
                throws IOException {
            output.writeInt(header.length + body.length);
            output.write(header);
            output.write(body);
            output.flush();
        }

        private static byte[] encodeRequestHeader(short apiKey, int correlationId, String clientId)
                throws IOException {
            java.io.ByteArrayOutputStream byteArrayOutputStream = new java.io.ByteArrayOutputStream();
            DataOutputStream output = new DataOutputStream(byteArrayOutputStream);
            output.writeShort(apiKey);
            output.writeShort(0);
            output.writeShort(2);
            output.writeInt(correlationId);
            writeNullableString(output, clientId);
            writeNullableString(output, UUID.randomUUID().toString().replace("-", ""));
            writeNullableString(output, "0000000000000001");
            output.writeByte(1);
            writeNullableString(output, "p0-tenant");
            writeNullableString(output, "p0-quota");
            writeNullableString(output, "p0-auth");
            output.writeByte(0);
            writeNullableString(output, null);
            output.writeShort(0);
            output.flush();
            return byteArrayOutputStream.toByteArray();
        }

        private static byte[] encodeProduceRequestBody(
                String topic, int partition, byte[] records, int timeoutMs) throws IOException {
            java.io.ByteArrayOutputStream byteArrayOutputStream = new java.io.ByteArrayOutputStream();
            DataOutputStream output = new DataOutputStream(byteArrayOutputStream);
            writeNullableString(output, null);
            output.writeShort(-1);
            output.writeInt(timeoutMs);
            output.writeInt(1);
            writeNullableString(output, topic);
            output.writeInt(1);
            output.writeInt(partition);
            writeBytes(output, records);
            output.flush();
            return byteArrayOutputStream.toByteArray();
        }

        private static byte[] encodeFetchRequestBody(
                String topic, int partition, long fetchOffset, int maxBytes) throws IOException {
            java.io.ByteArrayOutputStream byteArrayOutputStream = new java.io.ByteArrayOutputStream();
            DataOutputStream output = new DataOutputStream(byteArrayOutputStream);
            output.writeInt(-1);
            output.writeInt(500);
            output.writeInt(1);
            output.writeInt(maxBytes);
            output.writeByte(0);
            output.writeInt(0);
            output.writeInt(1);
            writeNullableString(output, topic);
            output.writeInt(1);
            output.writeInt(partition);
            output.writeInt(0);
            output.writeLong(fetchOffset);
            output.writeLong(0);
            output.writeInt(maxBytes);
            output.flush();
            return byteArrayOutputStream.toByteArray();
        }

        private static void readResponseHeader(DataInputStream input, int expectedCorrelationId)
                throws IOException {
            int frameLength = input.readInt();
            if (frameLength <= 0) {
                throw new IllegalStateException("Invalid frame length " + frameLength);
            }
            int correlationId = input.readInt();
            if (correlationId != expectedCorrelationId) {
                throw new IllegalStateException(
                        "Unexpected correlationId " + correlationId + ", expected " + expectedCorrelationId);
            }
            input.readShort();
            short errorCode = input.readShort();
            input.readInt();
            if (errorCode != ErrorCode.NONE.code()) {
                throw new IllegalStateException("Response header errorCode=" + errorCode);
            }
        }

        private static String readNullableString(DataInputStream input) throws IOException {
            short length = input.readShort();
            if (length < 0) {
                return null;
            }
            byte[] bytes = input.readNBytes(length);
            if (bytes.length != length) {
                throw new IllegalStateException("Unexpected string length");
            }
            return new String(bytes, StandardCharsets.UTF_8);
        }

        private static byte[] readBytes(DataInputStream input) throws IOException {
            int length = input.readInt();
            if (length < 0) {
                throw new IllegalStateException("Negative bytes length " + length);
            }
            byte[] bytes = input.readNBytes(length);
            if (bytes.length != length) {
                throw new IllegalStateException("Unexpected bytes length");
            }
            return bytes;
        }

        private static void writeNullableString(DataOutputStream output, String value)
                throws IOException {
            if (value == null) {
                output.writeShort(-1);
                return;
            }
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            output.writeShort(bytes.length);
            output.write(bytes);
        }

        private static void writeBytes(DataOutputStream output, byte[] value) throws IOException {
            output.writeInt(value.length);
            output.write(value);
        }
    }
}
