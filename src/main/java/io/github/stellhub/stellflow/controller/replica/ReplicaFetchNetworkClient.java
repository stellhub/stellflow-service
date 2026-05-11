package io.github.stellhub.stellflow.controller.replica;

import io.github.stellhub.stellflow.network.protocol.ApiKey;
import io.github.stellhub.stellflow.network.protocol.ErrorCode;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;

/**
 * 面向 leader 的长连接副本抓取客户端，支持多分区共享连接和 in-flight correlation pipeline。
 */
@Slf4j
public class ReplicaFetchNetworkClient implements Closeable {

    private static final short HEADER_VERSION = 2;

    private final ReplicaFetchConfig config;
    private final ReplicaFetchConnectionKey connectionKey;
    private final Map<Integer, CompletableFuture<ReplicaFetchResult>> pending =
            new ConcurrentHashMap<>();
    private final AtomicInteger nextCorrelationId = new AtomicInteger(100_000);

    private Socket socket;
    private DataInputStream input;
    private DataOutputStream output;
    private Thread readerThread;
    private volatile boolean closed;

    public ReplicaFetchNetworkClient(
            ReplicaFetchConfig config, ReplicaFetchConnectionKey connectionKey) {
        this.config = config;
        this.connectionKey = connectionKey;
    }

    /**
     * 在共享连接上提交单次分区抓取请求。
     */
    public ReplicaFetchResult fetch(
            String topic,
            int partition,
            int currentLeaderEpoch,
            long fetchOffset) {
        int correlationId = nextCorrelationId.incrementAndGet();
        try {
            ensureConnected();
            CompletableFuture<ReplicaFetchResult> future = new CompletableFuture<>();
            pending.put(correlationId, future);
            synchronized (this) {
                writeFrame(
                        encodeRequestHeader(correlationId),
                        encodeFetchRequestBody(
                                topic,
                                partition,
                                config.getFollowerBrokerId(),
                                currentLeaderEpoch,
                                fetchOffset,
                                config.getMaxBytes(),
                                config.getMaxWaitMs(),
                                config.getMinBytes()));
            }
            return future.join();
        } catch (IOException | RuntimeException exception) {
            pending.remove(correlationId);
            closeQuietly();
            throw new IllegalStateException(
                    "Replica fetch request failed for "
                            + topic
                            + "-"
                            + partition
                            + " via "
                            + connectionKey,
                    exception);
        }
    }

    private synchronized void ensureConnected() throws IOException {
        if (socket != null && socket.isConnected() && !socket.isClosed()) {
            return;
        }
        socket = new Socket();
        socket.connect(
                new InetSocketAddress(connectionKey.leaderHost(), connectionKey.leaderPort()),
                config.getConnectTimeoutMs());
        socket.setSoTimeout(config.getSocketTimeoutMs());
        input = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        output = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
        closed = false;
        readerThread =
                new Thread(this::readLoop, "stellflow-replica-fetch-reader-" + connectionKey.leaderBrokerId());
        readerThread.setDaemon(true);
        readerThread.start();
    }

    private void readLoop() {
        try {
            while (!closed) {
                int frameLength = input.readInt();
                if (frameLength <= 0) {
                    throw new IllegalStateException(
                            "Invalid replica fetch response frame length " + frameLength);
                }
                int correlationId = input.readInt();
                ReplicaFetchResult result = decodeFetchResponse(correlationId);
                CompletableFuture<ReplicaFetchResult> future = pending.remove(correlationId);
                if (future != null) {
                    future.complete(result);
                }
            }
        } catch (IOException | RuntimeException exception) {
            if (!closed) {
                failAll(exception);
                closeQuietly();
            }
        }
    }

    private ReplicaFetchResult decodeFetchResponse(int correlationId) throws IOException {
        short headerVersion = input.readShort();
        if (headerVersion != HEADER_VERSION) {
            throw new IllegalStateException("Unexpected response header version " + headerVersion);
        }
        ErrorCode topLevelError = ErrorCode.fromCode(input.readShort());
        int throttleTimeMs = input.readInt();
        if (throttleTimeMs < 0) {
            throw new IllegalStateException("Invalid throttleTimeMs " + throttleTimeMs);
        }
        if (topLevelError != ErrorCode.NONE) {
            return new ReplicaFetchResult(topLevelError, 0, 0, 0, 0, new byte[0]);
        }

        int sessionId = input.readInt();
        if (sessionId != 0) {
            throw new IllegalStateException("Unexpected replica fetch sessionId " + sessionId);
        }
        int topicCount = input.readInt();
        if (topicCount != 1) {
            throw new IllegalStateException("Replica fetch must return exactly one topic");
        }
        readNullableString(input);
        int partitionCount = input.readInt();
        if (partitionCount != 1) {
            throw new IllegalStateException("Replica fetch must return exactly one partition");
        }
        input.readInt();
        ErrorCode errorCode = ErrorCode.fromCode(input.readShort());
        long highWatermark = input.readLong();
        long logStartOffset = input.readLong();
        long lastStableOffset = input.readLong();
        int abortedTransactionCount = input.readInt();
        for (int index = 0; index < abortedTransactionCount; index++) {
            input.readLong();
            input.readLong();
        }
        byte[] records = readBytes(input);
        return new ReplicaFetchResult(
                errorCode,
                0,
                highWatermark,
                logStartOffset,
                lastStableOffset,
                records == null ? new byte[0] : records);
    }

    private byte[] encodeRequestHeader(int correlationId) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream headerOutput = new DataOutputStream(byteArrayOutputStream);
        headerOutput.writeShort(ApiKey.FETCH.code());
        headerOutput.writeShort(0);
        headerOutput.writeShort(HEADER_VERSION);
        headerOutput.writeInt(correlationId);
        writeNullableString(headerOutput, "stellflow-replica-fetcher");
        writeNullableString(headerOutput, null);
        writeNullableString(headerOutput, null);
        headerOutput.writeByte(0);
        writeNullableString(headerOutput, null);
        writeNullableString(headerOutput, "replica-fetch");
        writeNullableString(headerOutput, null);
        headerOutput.writeByte(2);
        writeNullableString(headerOutput, "replica-sync");
        headerOutput.writeShort(0);
        headerOutput.flush();
        return byteArrayOutputStream.toByteArray();
    }

    private byte[] encodeFetchRequestBody(
            String topic,
            int partition,
            int followerBrokerId,
            int currentLeaderEpoch,
            long fetchOffset,
            int maxBytes,
            int maxWaitMs,
            int minBytes)
            throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream bodyOutput = new DataOutputStream(byteArrayOutputStream);
        bodyOutput.writeInt(followerBrokerId);
        bodyOutput.writeInt(maxWaitMs);
        bodyOutput.writeInt(minBytes);
        bodyOutput.writeInt(maxBytes);
        bodyOutput.writeByte(0);
        bodyOutput.writeInt(0);
        bodyOutput.writeInt(1);
        writeNullableString(bodyOutput, topic);
        bodyOutput.writeInt(1);
        bodyOutput.writeInt(partition);
        bodyOutput.writeInt(currentLeaderEpoch);
        bodyOutput.writeLong(fetchOffset);
        bodyOutput.writeLong(0);
        bodyOutput.writeInt(maxBytes);
        bodyOutput.flush();
        return byteArrayOutputStream.toByteArray();
    }

    private void writeFrame(byte[] header, byte[] body) throws IOException {
        output.writeInt(header.length + body.length);
        output.write(header);
        output.write(body);
        output.flush();
    }

    private void failAll(Throwable throwable) {
        for (CompletableFuture<ReplicaFetchResult> future : pending.values()) {
            future.completeExceptionally(throwable);
        }
        pending.clear();
    }

    private void closeQuietly() {
        try {
            close();
        } catch (IOException ignored) {
            // Ignore close failures after network errors.
        }
    }

    @Override
    public synchronized void close() throws IOException {
        closed = true;
        IOException first = null;
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException exception) {
                first = exception;
            } finally {
                socket = null;
            }
        }
        if (input != null) {
            try {
                input.close();
            } catch (IOException exception) {
                if (first == null) {
                    first = exception;
                }
            } finally {
                input = null;
            }
        }
        if (output != null) {
            try {
                output.close();
            } catch (IOException exception) {
                if (first == null) {
                    first = exception;
                }
            } finally {
                output = null;
            }
        }
        failAll(new IllegalStateException("Replica fetch connection closed"));
        if (first != null) {
            throw first;
        }
    }

    private void writeNullableString(DataOutputStream output, String value) throws IOException {
        if (value == null) {
            output.writeShort(-1);
            return;
        }
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        output.writeShort(bytes.length);
        output.write(bytes);
    }

    private String readNullableString(DataInputStream input) throws IOException {
        short length = input.readShort();
        if (length < 0) {
            return null;
        }
        byte[] bytes = input.readNBytes(length);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private byte[] readBytes(DataInputStream input) throws IOException {
        int length = input.readInt();
        if (length < 0) {
            return null;
        }
        return input.readNBytes(length);
    }
}
