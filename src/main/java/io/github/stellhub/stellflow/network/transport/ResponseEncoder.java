package io.github.stellhub.stellflow.network.transport;

import io.github.stellhub.stellflow.network.protocol.HeaderCodec;
import io.github.stellhub.stellflow.network.protocol.ProtocolCodecRegistry;
import io.github.stellhub.stellflow.server.api.ResponseContext;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
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

            ByteBuf frame = ctx.alloc().buffer(Integer.BYTES + payload.readableBytes());
            frame.writeInt(payload.readableBytes());
            frame.writeBytes(payload);
            out.add(frame);
        } finally {
            payload.release();
        }
    }
}
