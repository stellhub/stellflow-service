package io.github.stellhub.stellflow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.stellhub.stellflow.network.protocol.ApiKey;
import io.github.stellhub.stellflow.network.protocol.ErrorCode;
import io.github.stellhub.stellflow.network.protocol.ProtocolCodecRegistry;
import io.github.stellhub.stellflow.network.transport.NettyTransportConfig;
import io.github.stellhub.stellflow.network.transport.SocketServer;
import io.github.stellhub.stellflow.server.api.BrokerApis;
import io.github.stellhub.stellflow.server.api.InMemoryRequestChannel;
import io.github.stellhub.stellflow.server.api.RequestChannel;
import io.github.stellhub.stellflow.server.api.RequestDispatcher;
import io.github.stellhub.stellflow.server.api.ResponseResponder;
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
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * 数据面协议冒烟测试。
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ProtocolSmokeTest {

    private static SocketServer socketServer;
    private static RequestDispatcher requestDispatcher;
    private static ResponseResponder responseResponder;
    private static BrokerApis brokerApis;
    private static Path logDir;
    private static int port;

    /**
     * 启动最小 Broker 骨架。
     */
    @BeforeAll
    static void beforeAll() throws Exception {
        port = findFreePort();
        logDir = Files.createTempDirectory("stellflow-protocol-test-");

        RequestChannel requestChannel = new InMemoryRequestChannel();
        brokerApis = BrokerApis.defaultBrokerApis("127.0.0.1", port, logDir);
        requestDispatcher =
                new RequestDispatcher(
                        requestChannel,
                        brokerApis,
                        2);
        responseResponder = new ResponseResponder(requestChannel);
        socketServer =
                new SocketServer(
                        NettyTransportConfig.builder().host("127.0.0.1").port(port).build(),
                        ProtocolCodecRegistry.defaultRegistry(),
                        requestChannel);

        requestDispatcher.start();
        responseResponder.start();
        socketServer.start();
    }

    /**
     * 关闭最小 Broker 骨架。
     */
    @AfterAll
    static void afterAll() {
        if (socketServer != null) {
            socketServer.close();
        }
        if (responseResponder != null) {
            responseResponder.close();
        }
        if (requestDispatcher != null) {
            requestDispatcher.close();
        }
        if (brokerApis != null) {
            brokerApis.close();
        }
        deleteDirectory(logDir);
    }

    /**
     * 验证 ApiVersions 链路可用。
     */
    @Test
    @Order(1)
    void shouldRoundTripApiVersionsRequest() throws Exception {
        try (Socket socket = new Socket("127.0.0.1", port)) {
            socket.setSoTimeout(3000);
            DataOutputStream output =
                    new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            DataInputStream input =
                    new DataInputStream(new BufferedInputStream(socket.getInputStream()));

            byte[] body =
                    encodeApiVersionsRequestBody(
                            "stellflow-java-test", "0.0.1-test", List.of("fetch.long_poll"));
            byte[] header =
                    encodeRequestHeader(
                            ApiKey.API_VERSIONS.code(),
                            (short) 0,
                            (short) 2,
                            10001,
                            "stellflow-test-client",
                            "4bf92f3577b34da6a3ce929d0e0e4736",
                            "00f067aa0ba902b7",
                            (byte) 1,
                            "tenant-test",
                            "quota-test",
                            "authctx-test",
                            (byte) 0,
                            null,
                            (short) 0);

            writeFrame(output, header, body);

            ResponseHeaderView responseHeader = readResponseHeader(input);
            assertEquals(10001, responseHeader.correlationId());
            assertEquals(2, responseHeader.headerVersion());
            assertEquals(0, responseHeader.errorCode());
            assertEquals(0, responseHeader.throttleTimeMs());

            int apiCount = input.readInt();
            assertTrue(apiCount >= 2);

            boolean sawApiVersions = false;
            boolean sawMetadata = false;
            for (int index = 0; index < apiCount; index++) {
                short apiKey = input.readShort();
                short minVersion = input.readShort();
                short maxVersion = input.readShort();
                if (apiKey == ApiKey.API_VERSIONS.code()) {
                    sawApiVersions = true;
                    assertEquals(0, minVersion);
                    assertEquals(0, maxVersion);
                }
                if (apiKey == ApiKey.METADATA.code()) {
                    sawMetadata = true;
                }
            }

            assertTrue(sawApiVersions);
            assertTrue(sawMetadata);
            assertEquals("stellflow-broker", readNullableString(input));
            assertEquals("0.0.1-SNAPSHOT", readNullableString(input));
            List<String> features = readStringArray(input);
            assertTrue(features.contains("fetch.long_poll"));
        }
    }

    /**
     * 验证空 topics 的 Metadata 链路可用。
     */
    @Test
    @Order(2)
    void shouldReturnPlaceholderMetadataForEmptyTopics() throws Exception {
        try (Socket socket = new Socket("127.0.0.1", port)) {
            socket.setSoTimeout(3000);
            DataOutputStream output =
                    new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            DataInputStream input =
                    new DataInputStream(new BufferedInputStream(socket.getInputStream()));

            byte[] body = encodeMetadataRequestBody(List.of(), false, false, false);
            byte[] header =
                    encodeRequestHeader(
                            ApiKey.METADATA.code(),
                            (short) 0,
                            (short) 2,
                            10002,
                            "stellflow-test-client",
                            "4bf92f3577b34da6a3ce929d0e0e4737",
                            "00f067aa0ba902b8",
                            (byte) 1,
                            "tenant-test",
                            "quota-test-read",
                            "authctx-test",
                            (byte) 0,
                            null,
                            (short) 0);

            writeFrame(output, header, body);

            ResponseHeaderView responseHeader = readResponseHeader(input);
            assertEquals(10002, responseHeader.correlationId());
            assertEquals(0, responseHeader.errorCode());

            assertEquals("stellflow-dev-cluster", readNullableString(input));
            assertEquals(0, input.readInt());

            int brokerCount = input.readInt();
            assertEquals(1, brokerCount);
            assertEquals(0, input.readInt());
            assertEquals("127.0.0.1", readNullableString(input));
            assertEquals(port, input.readInt());
            assertEquals(null, readNullableString(input));

            int topicCount = input.readInt();
            assertEquals(1, topicCount);
            assertEquals(0, input.readShort());
            assertEquals("__empty__", readNullableString(input));
            assertEquals(1, input.readByte());

            int partitionCount = input.readInt();
            assertEquals(1, partitionCount);
            assertEquals(0, input.readShort());
            assertEquals(0, input.readInt());
            assertEquals(0, input.readInt());
            assertEquals(0, input.readInt());

            assertEquals(List.of(0), readIntArray(input));
            assertEquals(List.of(0), readIntArray(input));
            assertTrue(readIntArray(input).isEmpty());
            assertEquals(0, input.readInt());
            assertEquals(0, input.readInt());
        }
    }

    /**
     * 验证指定 topic 的 Metadata 返回未知 topic 错误。
     */
    @Test
    @Order(3)
    void shouldReturnUnknownTopicForNamedMetadataRequest() throws Exception {
        try (Socket socket = new Socket("127.0.0.1", port)) {
            socket.setSoTimeout(3000);
            DataOutputStream output =
                    new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            DataInputStream input =
                    new DataInputStream(new BufferedInputStream(socket.getInputStream()));

            byte[] body = encodeMetadataRequestBody(List.of("orders"), false, false, false);
            byte[] header =
                    encodeRequestHeader(
                            ApiKey.METADATA.code(),
                            (short) 0,
                            (short) 2,
                            10003,
                            "stellflow-test-client",
                            "4bf92f3577b34da6a3ce929d0e0e4738",
                            "00f067aa0ba902b9",
                            (byte) 1,
                            "tenant-test",
                            "quota-test-read",
                            "authctx-test",
                            (byte) 1,
                            "exp-metadata-a",
                            (short) 0);

            writeFrame(output, header, body);

            ResponseHeaderView responseHeader = readResponseHeader(input);
            assertEquals(10003, responseHeader.correlationId());
            assertEquals(0, responseHeader.errorCode());

            assertEquals("stellflow-dev-cluster", readNullableString(input));
            assertEquals(0, input.readInt());

            int brokerCount = input.readInt();
            assertEquals(1, brokerCount);
            input.readInt();
            readNullableString(input);
            input.readInt();
            readNullableString(input);

            int topicCount = input.readInt();
            assertEquals(1, topicCount);
            assertEquals(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION.code(), input.readShort());
            assertEquals("orders", readNullableString(input));
            assertEquals(0, input.readByte());
            assertEquals(0, input.readInt());
            assertEquals(0, input.readInt());
            assertEquals(0, input.readInt());
        }
    }

    /**
     * 验证 Produce / Fetch 主链路可用。
     */
    @Test
    @Order(4)
    void shouldRoundTripProduceAndFetch() throws Exception {
        byte[] producedRecords = "batch-records-v0".getBytes(StandardCharsets.UTF_8);

        try (Socket socket = new Socket("127.0.0.1", port)) {
            socket.setSoTimeout(3000);
            DataOutputStream output =
                    new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            DataInputStream input =
                    new DataInputStream(new BufferedInputStream(socket.getInputStream()));

            byte[] produceBody =
                    encodeProduceRequestBody("orders-main-chain", 0, producedRecords, (short) -1, 30000);
            byte[] produceHeader =
                    encodeRequestHeader(
                            ApiKey.PRODUCE.code(),
                            (short) 0,
                            (short) 2,
                            10004,
                            "stellflow-test-producer",
                            "4bf92f3577b34da6a3ce929d0e0e4740",
                            "00f067aa0ba90300",
                            (byte) 1,
                            "tenant-test",
                            "quota-test-write",
                            "authctx-test",
                            (byte) 1,
                            "exp-main-chain-a",
                            (short) 0);

            writeFrame(output, produceHeader, produceBody);

            ResponseHeaderView produceResponseHeader = readResponseHeader(input);
            assertEquals(10004, produceResponseHeader.correlationId());
            assertEquals(0, produceResponseHeader.errorCode());

            int produceTopicCount = input.readInt();
            assertEquals(1, produceTopicCount);
            assertEquals("orders-main-chain", readNullableString(input));
            int producePartitionCount = input.readInt();
            assertEquals(1, producePartitionCount);
            assertEquals(0, input.readInt());
            assertEquals(ErrorCode.NONE.code(), input.readShort());
            assertEquals(0L, input.readLong());
            assertEquals(0, input.readInt());
            assertEquals(-1L, input.readLong());
            assertEquals(0L, input.readLong());
        }

        try (Socket socket = new Socket("127.0.0.1", port)) {
            socket.setSoTimeout(3000);
            DataOutputStream output =
                    new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            DataInputStream input =
                    new DataInputStream(new BufferedInputStream(socket.getInputStream()));

            byte[] fetchBody =
                    encodeFetchRequestBody("orders-main-chain", 0, 0L, 4 * 1024 * 1024);
            byte[] fetchHeader =
                    encodeRequestHeader(
                            ApiKey.FETCH.code(),
                            (short) 0,
                            (short) 2,
                            10005,
                            "stellflow-test-consumer",
                            "4bf92f3577b34da6a3ce929d0e0e4741",
                            "00f067aa0ba90301",
                            (byte) 1,
                            "tenant-test",
                            "quota-test-read",
                            "authctx-test",
                            (byte) 0,
                            null,
                            (short) 0);

            writeFrame(output, fetchHeader, fetchBody);

            ResponseHeaderView fetchResponseHeader = readResponseHeader(input);
            assertEquals(10005, fetchResponseHeader.correlationId());
            assertEquals(0, fetchResponseHeader.errorCode());

            assertEquals(0, input.readInt());
            int fetchTopicCount = input.readInt();
            assertEquals(1, fetchTopicCount);
            assertEquals("orders-main-chain", readNullableString(input));
            int fetchPartitionCount = input.readInt();
            assertEquals(1, fetchPartitionCount);
            assertEquals(0, input.readInt());
            assertEquals(ErrorCode.NONE.code(), input.readShort());
            assertEquals(1L, input.readLong());
            assertEquals(0L, input.readLong());
            assertEquals(1L, input.readLong());
            assertEquals(0, input.readInt());
            assertEquals("batch-records-v0", new String(readBytes(input), StandardCharsets.UTF_8));
        }
    }

    /**
     * 验证不支持版本时返回一致错误码。
     */
    @Test
    @Order(5)
    void shouldRejectUnsupportedProduceVersion() throws Exception {
        try (Socket socket = new Socket("127.0.0.1", port)) {
            socket.setSoTimeout(3000);
            DataOutputStream output =
                    new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            DataInputStream input =
                    new DataInputStream(new BufferedInputStream(socket.getInputStream()));

            byte[] produceBody =
                    encodeProduceRequestBody("orders-unsupported-version", 0, "v1".getBytes(StandardCharsets.UTF_8), (short) -1, 30000);
            byte[] produceHeader =
                    encodeRequestHeader(
                            ApiKey.PRODUCE.code(),
                            (short) 1,
                            (short) 2,
                            10006,
                            "stellflow-test-producer",
                            "4bf92f3577b34da6a3ce929d0e0e4742",
                            "00f067aa0ba90302",
                            (byte) 1,
                            "tenant-test",
                            "quota-test-write",
                            "authctx-test",
                            (byte) 0,
                            null,
                            (short) 0);

            writeFrame(output, produceHeader, produceBody);
            ResponseHeaderView responseHeader = readResponseHeader(input);
            assertEquals(10006, responseHeader.correlationId());
            assertEquals(ErrorCode.UNSUPPORTED_VERSION.code(), responseHeader.errorCode());
        }
    }

    /**
     * 验证非法帧会导致连接被关闭。
     */
    @Test
    @Order(6)
    void shouldCloseConnectionForIllegalFrame() throws Exception {
        try (Socket socket = new Socket("127.0.0.1", port)) {
            socket.setSoTimeout(3000);
            DataOutputStream output =
                    new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            DataInputStream input =
                    new DataInputStream(new BufferedInputStream(socket.getInputStream()));

            output.writeInt(-1);
            output.flush();

            assertConnectionClosed(input);
        }
    }

    /**
     * 验证超过 maxFrameLength 的帧会被拒绝。
     */
    @Test
    @Order(7)
    void shouldCloseConnectionForTooLargeFrame() throws Exception {
        withTemporaryServer(
                NettyTransportConfig.builder()
                        .host("127.0.0.1")
                        .port(findFreePort())
                        .maxFrameLength(64)
                        .build(),
                tempPort -> {
                    try (Socket socket = new Socket("127.0.0.1", tempPort)) {
                        socket.setSoTimeout(3000);
                        DataOutputStream output =
                                new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
                        DataInputStream input =
                                new DataInputStream(new BufferedInputStream(socket.getInputStream()));

                        byte[] oversized = new byte[96];
                        output.writeInt(oversized.length);
                        output.write(oversized);
                        output.flush();

                        assertConnectionClosed(input);
                    }
                });
    }

    /**
     * 验证空 records 会返回协议错误。
     */
    @Test
    @Order(8)
    void shouldRejectEmptyRecords() throws Exception {
        try (Socket socket = new Socket("127.0.0.1", port)) {
            socket.setSoTimeout(3000);
            DataOutputStream output =
                    new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            DataInputStream input =
                    new DataInputStream(new BufferedInputStream(socket.getInputStream()));

            byte[] produceBody = encodeProduceRequestBody("orders-empty-records", 0, new byte[0], (short) -1, 30000);
            byte[] produceHeader =
                    encodeRequestHeader(
                            ApiKey.PRODUCE.code(),
                            (short) 0,
                            (short) 2,
                            10007,
                            "stellflow-test-producer",
                            "4bf92f3577b34da6a3ce929d0e0e4743",
                            "00f067aa0ba90303",
                            (byte) 1,
                            "tenant-test",
                            "quota-test-write",
                            "authctx-test",
                            (byte) 0,
                            null,
                            (short) 0);

            writeFrame(output, produceHeader, produceBody);
            ResponseHeaderView responseHeader = readResponseHeader(input);
            assertEquals(10007, responseHeader.correlationId());
            assertEquals(ErrorCode.NONE.code(), responseHeader.errorCode());
            assertEquals(1, input.readInt());
            assertEquals("orders-empty-records", readNullableString(input));
            assertEquals(1, input.readInt());
            assertEquals(0, input.readInt());
            assertEquals(ErrorCode.INVALID_RECORD.code(), input.readShort());
            assertEquals(-1L, input.readLong());
        }
    }

    /**
     * 验证 topic 多分区批量写入与批量拉取。
     */
    @Test
    @Order(9)
    void shouldSupportMultiPartitionProduceAndFetch() throws Exception {
        try (Socket socket = new Socket("127.0.0.1", port)) {
            socket.setSoTimeout(3000);
            DataOutputStream output =
                    new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            DataInputStream input =
                    new DataInputStream(new BufferedInputStream(socket.getInputStream()));

            byte[] produceBody =
                    encodeProduceRequestBody(
                            "orders-batch",
                            List.of(
                                    new PartitionRecords(0, "p0-record".getBytes(StandardCharsets.UTF_8)),
                                    new PartitionRecords(1, "p1-record".getBytes(StandardCharsets.UTF_8))),
                            (short) -1,
                            30000);
            byte[] produceHeader =
                    encodeRequestHeader(
                            ApiKey.PRODUCE.code(),
                            (short) 0,
                            (short) 2,
                            10008,
                            "stellflow-test-producer",
                            "4bf92f3577b34da6a3ce929d0e0e4744",
                            "00f067aa0ba90304",
                            (byte) 1,
                            "tenant-test",
                            "quota-test-write",
                            "authctx-test",
                            (byte) 1,
                            "exp-batch-write-a",
                            (short) 0);

            writeFrame(output, produceHeader, produceBody);
            ResponseHeaderView responseHeader = readResponseHeader(input);
            assertEquals(10008, responseHeader.correlationId());
            assertEquals(ErrorCode.NONE.code(), responseHeader.errorCode());
            assertEquals(1, input.readInt());
            assertEquals("orders-batch", readNullableString(input));
            assertEquals(2, input.readInt());
        }

        try (Socket socket = new Socket("127.0.0.1", port)) {
            socket.setSoTimeout(3000);
            DataOutputStream output =
                    new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            DataInputStream input =
                    new DataInputStream(new BufferedInputStream(socket.getInputStream()));

            byte[] fetchBody =
                    encodeFetchRequestBody(
                            "orders-batch",
                            List.of(new FetchPartitionSpec(0, 0L, 4096), new FetchPartitionSpec(1, 0L, 4096)));
            byte[] fetchHeader =
                    encodeRequestHeader(
                            ApiKey.FETCH.code(),
                            (short) 0,
                            (short) 2,
                            10009,
                            "stellflow-test-consumer",
                            "4bf92f3577b34da6a3ce929d0e0e4745",
                            "00f067aa0ba90305",
                            (byte) 1,
                            "tenant-test",
                            "quota-test-read",
                            "authctx-test",
                            (byte) 0,
                            null,
                            (short) 0);

            writeFrame(output, fetchHeader, fetchBody);
            ResponseHeaderView responseHeader = readResponseHeader(input);
            assertEquals(10009, responseHeader.correlationId());
            assertEquals(ErrorCode.NONE.code(), responseHeader.errorCode());
            assertEquals(0, input.readInt());
            assertEquals(1, input.readInt());
            assertEquals("orders-batch", readNullableString(input));
            assertEquals(2, input.readInt());

            assertEquals(0, input.readInt());
            assertEquals(ErrorCode.NONE.code(), input.readShort());
            assertEquals(1L, input.readLong());
            assertEquals(0L, input.readLong());
            assertEquals(1L, input.readLong());
            assertEquals(0, input.readInt());
            assertEquals("p0-record", new String(readBytes(input), StandardCharsets.UTF_8));

            assertEquals(1, input.readInt());
            assertEquals(ErrorCode.NONE.code(), input.readShort());
            assertEquals(1L, input.readLong());
            assertEquals(0L, input.readLong());
            assertEquals(1L, input.readLong());
            assertEquals(0, input.readInt());
            assertEquals("p1-record", new String(readBytes(input), StandardCharsets.UTF_8));
        }
    }

    /**
     * 验证同一分区多次 Produce 后，Fetch 可按多批次返回。
     */
    @Test
    @Order(10)
    void shouldFetchMultipleBatchesFromSamePartition() throws Exception {
        produceSinglePartition("orders-multi-batch", 0, 10010, "batch-a", "exp-multi-batch-a");
        produceSinglePartition("orders-multi-batch", 0, 10011, "batch-b", "exp-multi-batch-b");

        try (Socket socket = new Socket("127.0.0.1", port)) {
            socket.setSoTimeout(3000);
            DataOutputStream output =
                    new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            DataInputStream input =
                    new DataInputStream(new BufferedInputStream(socket.getInputStream()));

            byte[] fetchBody =
                    encodeFetchRequestBody("orders-multi-batch", 0, 0L, 4 * 1024 * 1024);
            byte[] fetchHeader =
                    encodeRequestHeader(
                            ApiKey.FETCH.code(),
                            (short) 0,
                            (short) 2,
                            10012,
                            "stellflow-test-consumer",
                            "4bf92f3577b34da6a3ce929d0e0e4746",
                            "00f067aa0ba90306",
                            (byte) 1,
                            "tenant-test",
                            "quota-test-read",
                            "authctx-test",
                            (byte) 1,
                            "exp-multi-batch-read",
                            (short) 0);

            writeFrame(output, fetchHeader, fetchBody);

            ResponseHeaderView responseHeader = readResponseHeader(input);
            assertEquals(10012, responseHeader.correlationId());
            assertEquals(ErrorCode.NONE.code(), responseHeader.errorCode());
            assertEquals(0, input.readInt());
            assertEquals(1, input.readInt());
            assertEquals("orders-multi-batch", readNullableString(input));
            assertEquals(1, input.readInt());
            assertEquals(0, input.readInt());
            assertEquals(ErrorCode.NONE.code(), input.readShort());
            assertEquals(2L, input.readLong());
            assertEquals(0L, input.readLong());
            assertEquals(2L, input.readLong());
            assertEquals(0, input.readInt());
            assertEquals("batch-abatch-b", new String(readBytes(input), StandardCharsets.UTF_8));
        }
    }

    /**
     * 验证 ListOffsets 可查询 earliest、latest 和按时间查找。
     */
    @Test
    @Order(11)
    void shouldSupportListOffsetsQueries() throws Exception {
        produceSinglePartition("orders-offsets", 0, 10013, "offset-a", "exp-offset-a");
        Thread.sleep(5);
        long middle = System.currentTimeMillis();
        Thread.sleep(5);
        produceSinglePartition("orders-offsets", 0, 10014, "offset-b", "exp-offset-b");

        try (Socket socket = new Socket("127.0.0.1", port)) {
            socket.setSoTimeout(3000);
            DataOutputStream output =
                    new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            DataInputStream input =
                    new DataInputStream(new BufferedInputStream(socket.getInputStream()));

            byte[] listOffsetsBody =
                    encodeListOffsetsRequestBody(
                            "orders-offsets",
                            List.of(
                                    new ListOffsetsPartitionSpec(0, -2L, 2),
                                    new ListOffsetsPartitionSpec(0, middle, 2),
                                    new ListOffsetsPartitionSpec(0, -1L, 1)));
            byte[] header =
                    encodeRequestHeader(
                            ApiKey.LIST_OFFSETS.code(),
                            (short) 0,
                            (short) 2,
                            10015,
                            "stellflow-test-consumer",
                            "4bf92f3577b34da6a3ce929d0e0e4748",
                            "00f067aa0ba90308",
                            (byte) 1,
                            "tenant-test",
                            "quota-test-read",
                            "authctx-test",
                            (byte) 0,
                            null,
                            (short) 0);

            writeFrame(output, header, listOffsetsBody);

            ResponseHeaderView responseHeader = readResponseHeader(input);
            assertEquals(10015, responseHeader.correlationId());
            assertEquals(ErrorCode.NONE.code(), responseHeader.errorCode());
            assertEquals(1, input.readInt());
            assertEquals("orders-offsets", readNullableString(input));
            assertEquals(3, input.readInt());

            assertEquals(0, input.readInt());
            assertEquals(ErrorCode.NONE.code(), input.readShort());
            input.readInt();
            input.readLong();
            assertEquals(0L, input.readLong());
            assertEquals(2, input.readInt());
            assertEquals(0L, input.readLong());
            assertEquals(1L, input.readLong());

            assertEquals(0, input.readInt());
            assertEquals(ErrorCode.NONE.code(), input.readShort());
            input.readInt();
            long matchedTimestamp = input.readLong();
            assertTrue(matchedTimestamp >= middle);
            assertEquals(1L, input.readLong());
            assertEquals(1, input.readInt());
            assertEquals(1L, input.readLong());

            assertEquals(0, input.readInt());
            assertEquals(ErrorCode.NONE.code(), input.readShort());
            input.readInt();
            input.readLong();
            assertEquals(2L, input.readLong());
            assertEquals(1, input.readInt());
            assertEquals(2L, input.readLong());
        }
    }

    /**
     * 查找空闲端口。
     */
    private static int findFreePort() throws IOException {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            return serverSocket.getLocalPort();
        }
    }

    /**
     * 写出完整帧。
     */
    private static void writeFrame(DataOutputStream output, byte[] header, byte[] body)
            throws IOException {
        output.writeInt(header.length + body.length);
        output.write(header);
        output.write(body);
        output.flush();
    }

    /**
     * 发送单分区 Produce 请求。
     */
    private static void produceSinglePartition(
            String topic, int partition, int correlationId, String payload, String trafficTag)
            throws IOException {
        try (Socket socket = new Socket("127.0.0.1", port)) {
            socket.setSoTimeout(3000);
            DataOutputStream output =
                    new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            DataInputStream input =
                    new DataInputStream(new BufferedInputStream(socket.getInputStream()));

            byte[] produceBody =
                    encodeProduceRequestBody(
                            topic,
                            partition,
                            payload.getBytes(StandardCharsets.UTF_8),
                            (short) -1,
                            30000);
            byte[] produceHeader =
                    encodeRequestHeader(
                            ApiKey.PRODUCE.code(),
                            (short) 0,
                            (short) 2,
                            correlationId,
                            "stellflow-test-producer",
                            "4bf92f3577b34da6a3ce929d0e0e4747",
                            "00f067aa0ba90307",
                            (byte) 1,
                            "tenant-test",
                            "quota-test-write",
                            "authctx-test",
                            (byte) 1,
                            trafficTag,
                            (short) 0);

            writeFrame(output, produceHeader, produceBody);
            ResponseHeaderView responseHeader = readResponseHeader(input);
            assertEquals(correlationId, responseHeader.correlationId());
            assertEquals(ErrorCode.NONE.code(), responseHeader.errorCode());
        }
    }

    /**
     * 编码请求头。
     */
    private static byte[] encodeRequestHeader(
            short apiKey,
            short apiVersion,
            short headerVersion,
            int correlationId,
            String clientId,
            String traceId,
            String spanId,
            byte traceFlags,
            String tenantId,
            String quotaKey,
            String authContextId,
            byte trafficClass,
            String trafficTag,
            short flags)
            throws IOException {
        java.io.ByteArrayOutputStream byteArrayOutputStream = new java.io.ByteArrayOutputStream();
        DataOutputStream output = new DataOutputStream(byteArrayOutputStream);
        output.writeShort(apiKey);
        output.writeShort(apiVersion);
        output.writeShort(headerVersion);
        output.writeInt(correlationId);
        writeNullableString(output, clientId);
        writeNullableString(output, traceId);
        writeNullableString(output, spanId);
        output.writeByte(traceFlags);
        writeNullableString(output, tenantId);
        writeNullableString(output, quotaKey);
        writeNullableString(output, authContextId);
        output.writeByte(trafficClass);
        writeNullableString(output, trafficTag);
        output.writeShort(flags);
        output.flush();
        return byteArrayOutputStream.toByteArray();
    }

    /**
     * 编码 ApiVersions 请求体。
     */
    private static byte[] encodeApiVersionsRequestBody(
            String clientSoftwareName, String clientSoftwareVersion, List<String> supportedFeatures)
            throws IOException {
        java.io.ByteArrayOutputStream byteArrayOutputStream = new java.io.ByteArrayOutputStream();
        DataOutputStream output = new DataOutputStream(byteArrayOutputStream);
        writeNullableString(output, clientSoftwareName);
        writeNullableString(output, clientSoftwareVersion);
        writeStringArray(output, supportedFeatures);
        output.flush();
        return byteArrayOutputStream.toByteArray();
    }

    /**
     * 编码 Metadata 请求体。
     */
    private static byte[] encodeMetadataRequestBody(
            List<String> topics,
            boolean includeClusterAuthorizedOperations,
            boolean includeTopicAuthorizedOperations,
            boolean allowAutoTopicCreation)
            throws IOException {
        java.io.ByteArrayOutputStream byteArrayOutputStream = new java.io.ByteArrayOutputStream();
        DataOutputStream output = new DataOutputStream(byteArrayOutputStream);
        output.writeInt(topics.size());
        for (String topic : topics) {
            writeNullableString(output, topic);
        }
        output.writeByte(includeClusterAuthorizedOperations ? 1 : 0);
        output.writeByte(includeTopicAuthorizedOperations ? 1 : 0);
        output.writeByte(allowAutoTopicCreation ? 1 : 0);
        output.flush();
        return byteArrayOutputStream.toByteArray();
    }

    /**
     * 编码 Produce 请求体。
     */
    private static byte[] encodeProduceRequestBody(
            String topic, int partition, byte[] records, short acks, int timeoutMs) throws IOException {
        return encodeProduceRequestBody(topic, List.of(new PartitionRecords(partition, records)), acks, timeoutMs);
    }

    /**
     * 编码多分区 Produce 请求体。
     */
    private static byte[] encodeProduceRequestBody(
            String topic, List<PartitionRecords> partitions, short acks, int timeoutMs) throws IOException {
        java.io.ByteArrayOutputStream byteArrayOutputStream = new java.io.ByteArrayOutputStream();
        DataOutputStream output = new DataOutputStream(byteArrayOutputStream);
        writeNullableString(output, null);
        output.writeShort(acks);
        output.writeInt(timeoutMs);
        output.writeInt(1);
        writeNullableString(output, topic);
        output.writeInt(partitions.size());
        for (PartitionRecords partitionRecords : partitions) {
            output.writeInt(partitionRecords.partition());
            writeBytes(output, partitionRecords.records());
        }
        output.flush();
        return byteArrayOutputStream.toByteArray();
    }

    /**
     * 编码 Fetch 请求体。
     */
    private static byte[] encodeFetchRequestBody(
            String topic, int partition, long fetchOffset, int partitionMaxBytes) throws IOException {
        return encodeFetchRequestBody(
                topic, List.of(new FetchPartitionSpec(partition, fetchOffset, partitionMaxBytes)));
    }

    /**
     * 编码多分区 Fetch 请求体。
     */
    private static byte[] encodeFetchRequestBody(String topic, List<FetchPartitionSpec> partitions)
            throws IOException {
        java.io.ByteArrayOutputStream byteArrayOutputStream = new java.io.ByteArrayOutputStream();
        DataOutputStream output = new DataOutputStream(byteArrayOutputStream);
        output.writeInt(-1);
        output.writeInt(500);
        output.writeInt(1);
        output.writeInt(8 * 1024 * 1024);
        output.writeByte(0);
        output.writeInt(0);
        output.writeInt(1);
        writeNullableString(output, topic);
        output.writeInt(partitions.size());
        for (FetchPartitionSpec partition : partitions) {
            output.writeInt(partition.partition());
            output.writeInt(0);
            output.writeLong(partition.fetchOffset());
            output.writeLong(0);
            output.writeInt(partition.partitionMaxBytes());
        }
        output.flush();
        return byteArrayOutputStream.toByteArray();
    }

    /**
     * 编码 ListOffsets 请求体。
     */
    private static byte[] encodeListOffsetsRequestBody(
            String topic, List<ListOffsetsPartitionSpec> partitions) throws IOException {
        java.io.ByteArrayOutputStream byteArrayOutputStream = new java.io.ByteArrayOutputStream();
        DataOutputStream output = new DataOutputStream(byteArrayOutputStream);
        output.writeInt(-1);
        output.writeByte(0);
        output.writeInt(1);
        writeNullableString(output, topic);
        output.writeInt(partitions.size());
        for (ListOffsetsPartitionSpec partition : partitions) {
            output.writeInt(partition.partition());
            output.writeInt(0);
            output.writeLong(partition.timestamp());
            output.writeInt(partition.maxNumOffsets());
        }
        output.flush();
        return byteArrayOutputStream.toByteArray();
    }

    /**
     * 断言服务端快速关闭连接。
     */
    private static void assertConnectionClosed(DataInputStream input) throws IOException {
        int value = input.read();
        assertEquals(-1, value);
    }

    /**
     * 启动临时 Broker 并运行测试逻辑。
     */
    private static void withTemporaryServer(NettyTransportConfig config, ThrowingIntConsumer consumer)
            throws Exception {
        Path tempLogDir = Files.createTempDirectory("stellflow-protocol-test-temp-");
        RequestChannel requestChannel = new InMemoryRequestChannel();
        BrokerApis tempBrokerApis =
                BrokerApis.defaultBrokerApis("127.0.0.1", config.getPort(), tempLogDir);
        RequestDispatcher tempDispatcher =
                new RequestDispatcher(requestChannel, tempBrokerApis, 2);
        ResponseResponder tempResponder = new ResponseResponder(requestChannel);
        SocketServer tempServer = new SocketServer(config, ProtocolCodecRegistry.defaultRegistry(), requestChannel);
        try {
            tempDispatcher.start();
            tempResponder.start();
            tempServer.start();
            consumer.accept(config.getPort());
        } finally {
            tempServer.close();
            tempResponder.close();
            tempDispatcher.close();
            tempBrokerApis.close();
            deleteDirectory(tempLogDir);
        }
    }

    /**
     * 递归删除临时目录。
     */
    private static void deleteDirectory(Path directory) {
        if (directory == null || !Files.exists(directory)) {
            return;
        }
        try (Stream<Path> stream = Files.walk(directory)) {
            stream.sorted((left, right) -> right.getNameCount() - left.getNameCount())
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
            throw new IllegalStateException("Failed to walk temp directory " + directory, exception);
        }
    }

    /**
     * 读取响应头。
     */
    private static ResponseHeaderView readResponseHeader(DataInputStream input) throws IOException {
        int frameLength = input.readInt();
        assertTrue(frameLength > 0);
        int correlationId = input.readInt();
        short headerVersion = input.readShort();
        short errorCode = input.readShort();
        int throttleTimeMs = input.readInt();
        return new ResponseHeaderView(correlationId, headerVersion, errorCode, throttleTimeMs);
    }

    /**
     * 读取 nullable string。
     */
    private static String readNullableString(DataInputStream input) throws IOException {
        short length = input.readShort();
        if (length < 0) {
            return null;
        }
        byte[] bytes = input.readNBytes(length);
        assertEquals(length, bytes.length);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * 写入 nullable string。
     */
    private static void writeNullableString(DataOutputStream output, String value) throws IOException {
        if (value == null) {
            output.writeShort(-1);
            return;
        }
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        output.writeShort(bytes.length);
        output.write(bytes);
    }

    /**
     * 读取字符串数组。
     */
    private static List<String> readStringArray(DataInputStream input) throws IOException {
        int length = input.readInt();
        assertTrue(length >= 0);
        List<String> values = new ArrayList<>(length);
        for (int index = 0; index < length; index++) {
            values.add(readNullableString(input));
        }
        return values;
    }

    /**
     * 写入字符串数组。
     */
    private static void writeStringArray(DataOutputStream output, List<String> values)
            throws IOException {
        output.writeInt(values.size());
        for (String value : values) {
            writeNullableString(output, value);
        }
    }

    /**
     * 读取 int 数组。
     */
    private static List<Integer> readIntArray(DataInputStream input) throws IOException {
        int length = input.readInt();
        assertTrue(length >= 0);
        List<Integer> values = new ArrayList<>(length);
        for (int index = 0; index < length; index++) {
            values.add(input.readInt());
        }
        return values;
    }

    /**
     * 读取 bytes。
     */
    private static byte[] readBytes(DataInputStream input) throws IOException {
        int length = input.readInt();
        assertTrue(length >= 0);
        byte[] bytes = input.readNBytes(length);
        assertEquals(length, bytes.length);
        return bytes;
    }

    /**
     * 写入 bytes。
     */
    private static void writeBytes(DataOutputStream output, byte[] value) throws IOException {
        output.writeInt(value.length);
        output.write(value);
    }

    /**
     * 简化响应头视图。
     */
    private record ResponseHeaderView(
            int correlationId, short headerVersion, short errorCode, int throttleTimeMs) {}

    /**
     * 分区级 produce 载荷。
     */
    private record PartitionRecords(int partition, byte[] records) {}

    /**
     * 分区级 fetch 规格。
     */
    private record FetchPartitionSpec(int partition, long fetchOffset, int partitionMaxBytes) {}

    /**
     * ListOffsets 分区查询规格。
     */
    private record ListOffsetsPartitionSpec(int partition, long timestamp, int maxNumOffsets) {}

    /**
     * 可抛异常的端口消费者。
     */
    @FunctionalInterface
    private interface ThrowingIntConsumer {
        void accept(int value) throws Exception;
    }
}
