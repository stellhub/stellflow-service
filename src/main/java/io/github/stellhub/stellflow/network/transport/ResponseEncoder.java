package io.github.stellhub.stellflow.network.transport;

import io.github.stellhub.stellflow.network.protocol.HeaderCodec;
import io.github.stellhub.stellflow.network.protocol.ProtocolCodecRegistry;
import io.github.stellhub.stellflow.server.api.ResponseContext;
import io.github.stellhub.stellflow.server.api.ZeroCopyFileRegion;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultFileRegion;
import io.netty.handler.codec.MessageToMessageEncoder;
import java.util.List;
import lombok.RequiredArgsConstructor;

/**
 * 响应编码器。
 */
@RequiredArgsConstructor
public class ResponseEncoder extends MessageToMessageEncoder<ResponseContext> {

    private final ProtocolCodecRegistry protocolCodecRegistry;

    /**
     * 编码统一响应头和响应体。
     */
    @Override
    protected void encode(ChannelHandlerContext ctx, ResponseContext msg, List<Object> out) {
        ByteBuf payload = ctx.alloc().buffer();
        try {
            HeaderCodec.encodeResponseHeader(payload, msg.getResponseHeader());
            protocolCodecRegistry.encodeResponseBody(
                    msg.getApiKey(), msg.getApiVersion(), msg.getResponseBody(), payload);

            long zeroCopyBytes =
                    msg.getZeroCopyFileRegions().stream().mapToLong(ZeroCopyFileRegion::count).sum();
            long frameLength = payload.readableBytes() + zeroCopyBytes;
            if (frameLength > Integer.MAX_VALUE) {
                throw new IllegalStateException("Response frame is too large for length prefix: " + frameLength);
            }
            ByteBuf frame = ctx.alloc().buffer(Integer.BYTES + payload.readableBytes());
            frame.writeInt((int) frameLength);
            frame.writeBytes(payload);
            out.add(frame);
            for (ZeroCopyFileRegion region : msg.getZeroCopyFileRegions()) {
                out.add(toDefaultFileRegion(region));
            }
        } finally {
            payload.release();
        }
    }

    private DefaultFileRegion toDefaultFileRegion(ZeroCopyFileRegion region) {
        return new DefaultFileRegion(region.file().toFile(), region.position(), region.count());
    }
}
