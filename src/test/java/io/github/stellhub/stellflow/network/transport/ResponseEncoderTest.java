package io.github.stellhub.stellflow.network.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import io.github.stellhub.stellflow.network.protocol.ApiKey;
import io.github.stellhub.stellflow.network.protocol.EmptyResponseBody;
import io.github.stellhub.stellflow.network.protocol.ErrorCode;
import io.github.stellhub.stellflow.network.protocol.ProtocolCodecRegistry;
import io.github.stellhub.stellflow.network.protocol.ResponseHeader;
import io.github.stellhub.stellflow.server.api.RequestContext;
import io.github.stellhub.stellflow.server.api.ResponseContext;
import io.github.stellhub.stellflow.server.api.ZeroCopyFileRegion;
import io.netty.buffer.ByteBuf;
import io.netty.channel.DefaultFileRegion;
import io.netty.channel.embedded.EmbeddedChannel;
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
}
