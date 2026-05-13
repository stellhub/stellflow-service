package io.github.stellhub.stellflow.network.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import io.github.stellhub.stellflow.network.protocol.AbortedTransaction;
import io.github.stellhub.stellflow.network.protocol.ApiKey;
import io.github.stellhub.stellflow.network.protocol.EmptyResponseBody;
import io.github.stellhub.stellflow.network.protocol.ErrorCode;
import io.github.stellhub.stellflow.network.protocol.FetchPartitionResponse;
import io.github.stellhub.stellflow.network.protocol.FetchResponseBody;
import io.github.stellhub.stellflow.network.protocol.FetchTopicResponse;
import io.github.stellhub.stellflow.network.protocol.ProtocolCodecRegistry;
import io.github.stellhub.stellflow.network.protocol.ProtocolSerde;
import io.github.stellhub.stellflow.network.protocol.ResponseHeader;
import io.github.stellhub.stellflow.server.api.FetchRecordsFileRegion;
import io.github.stellhub.stellflow.server.api.RequestContext;
import io.github.stellhub.stellflow.server.api.ResponseContext;
import io.github.stellhub.stellflow.server.api.ZeroCopyFileRegion;
import io.netty.buffer.Unpooled;
import io.netty.buffer.ByteBuf;
import io.netty.channel.DefaultFileRegion;
import io.netty.channel.embedded.EmbeddedChannel;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * ResponseEncoder 测试。
 */
class ResponseEncoderTest {

    @TempDir private Path tempDir;

    /**
     * 验证响应编码器可以输出零拷贝文件区域。
     */
    @Test
    void shouldEmitDefaultFileRegionForZeroCopyResponse() throws Exception {
        Path file = tempDir.resolve("records.log");
        Files.writeString(file, "record-payload", StandardCharsets.UTF_8);
        ResponseContext context =
                ResponseContext.builder()
                        .requestContext(RequestContext.builder().correlationId(9).build())
                        .apiKey(ApiKey.FETCH)
                        .apiVersion((short) 0)
                        .responseHeader(new ResponseHeader(9, (short) 2, ErrorCode.NONE, 0))
                        .responseBody(EmptyResponseBody.INSTANCE)
                        .zeroCopyFileRegions(List.of(new ZeroCopyFileRegion(file, 0, 14)))
                        .build();
        EmbeddedChannel channel =
                new EmbeddedChannel(new ResponseEncoder(ProtocolCodecRegistry.defaultRegistry()));

        channel.writeOutbound(context);
        ByteBuf header = channel.readOutbound();
        Object region = channel.readOutbound();

        assertEquals(header.readableBytes() - Integer.BYTES + 14, header.readInt());
        assertInstanceOf(DefaultFileRegion.class, region);
        ((DefaultFileRegion) region).release();
        header.release();
        channel.finishAndReleaseAll();
    }

    /**
     * 验证 Fetch records 零拷贝区间会按协议字段位置交错编码。
     */
    @Test
    void shouldInterleaveFetchRecordFileRegionAtRecordsField() throws Exception {
        Path file = tempDir.resolve("fetch-records.log");
        Files.writeString(file, "first", StandardCharsets.UTF_8);
        ResponseContext context =
                ResponseContext.builder()
                        .requestContext(RequestContext.builder().correlationId(10).build())
                        .apiKey(ApiKey.FETCH)
                        .apiVersion((short) 0)
                        .responseHeader(new ResponseHeader(10, (short) 2, ErrorCode.NONE, 0))
                        .responseBody(
                                new FetchResponseBody(
                                        11,
                                        List.of(
                                                new FetchTopicResponse(
                                                        "orders",
                                                        List.of(
                                                                new FetchPartitionResponse(
                                                                        0,
                                                                        ErrorCode.NONE,
                                                                        1,
                                                                        0,
                                                                        1,
                                                                        List.<AbortedTransaction>of(),
                                                                        new byte[0]),
                                                                new FetchPartitionResponse(
                                                                        1,
                                                                        ErrorCode.NONE,
                                                                        1,
                                                                        0,
                                                                        1,
                                                                        List.<AbortedTransaction>of(),
                                                                        "tail"
                                                                                .getBytes(
                                                                                        StandardCharsets
                                                                                                .UTF_8)))))))
                        .fetchRecordsFileRegions(
                                List.of(
                                        new FetchRecordsFileRegion(
                                                "orders",
                                                0,
                                                5,
                                                List.of(new ZeroCopyFileRegion(file, 0, 5)))))
                        .build();
        EmbeddedChannel channel =
                new EmbeddedChannel(new ResponseEncoder(ProtocolCodecRegistry.defaultRegistry()));

        channel.writeOutbound(context);
        ByteBuf firstChunk = channel.readOutbound();
        Object fileRegion = channel.readOutbound();
        ByteBuf tailChunk = channel.readOutbound();

        assertInstanceOf(DefaultFileRegion.class, fileRegion);
        byte[] frame = materializeFrame(firstChunk, "first".getBytes(StandardCharsets.UTF_8), tailChunk);
        ByteBuf buffer = Unpooled.wrappedBuffer(frame);
        try {
            assertEquals(buffer.readableBytes() - Integer.BYTES, buffer.readInt());
            assertEquals(10, buffer.readInt());
            assertEquals(2, buffer.readShort());
            assertEquals(ErrorCode.NONE.code(), buffer.readShort());
            assertEquals(0, buffer.readInt());
            assertEquals(11, buffer.readInt());
            assertEquals(1, buffer.readInt());
            assertEquals("orders", ProtocolSerde.readNullableString(buffer));
            assertEquals(2, buffer.readInt());
            assertEquals(0, buffer.readInt());
            assertEquals(ErrorCode.NONE.code(), buffer.readShort());
            assertEquals(1L, buffer.readLong());
            assertEquals(0L, buffer.readLong());
            assertEquals(1L, buffer.readLong());
            assertEquals(0, buffer.readInt());
            assertEquals("first", new String(ProtocolSerde.readBytes(buffer), StandardCharsets.UTF_8));
            assertEquals(1, buffer.readInt());
            assertEquals(ErrorCode.NONE.code(), buffer.readShort());
            assertEquals(1L, buffer.readLong());
            assertEquals(0L, buffer.readLong());
            assertEquals(1L, buffer.readLong());
            assertEquals(0, buffer.readInt());
            assertEquals("tail", new String(ProtocolSerde.readBytes(buffer), StandardCharsets.UTF_8));
            assertEquals(0, buffer.readableBytes());
        } finally {
            buffer.release();
            ((DefaultFileRegion) fileRegion).release();
            channel.finishAndReleaseAll();
        }
    }

    private byte[] materializeFrame(ByteBuf firstChunk, byte[] fileBytes, ByteBuf tailChunk) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        byte[] firstBytes = new byte[firstChunk.readableBytes()];
        firstChunk.readBytes(firstBytes);
        byte[] tailBytes = new byte[tailChunk.readableBytes()];
        tailChunk.readBytes(tailBytes);
        outputStream.writeBytes(firstBytes);
        outputStream.writeBytes(fileBytes);
        outputStream.writeBytes(tailBytes);
        firstChunk.release();
        tailChunk.release();
        return outputStream.toByteArray();
    }
}
