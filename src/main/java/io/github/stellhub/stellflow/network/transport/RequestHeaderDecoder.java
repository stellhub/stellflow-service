package io.github.stellhub.stellflow.network.transport;

import io.github.stellhub.stellflow.network.protocol.HeaderCodec;
import io.github.stellhub.stellflow.network.protocol.RequestHeader;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

/**
 * 请求头解码器。
 */
@Slf4j
public class RequestHeaderDecoder extends MessageToMessageDecoder<ByteBuf> {

    /**
     * 解析统一请求头并保留请求体视图。
     */
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) {
        try {
            RequestHeader header = HeaderCodec.decodeRequestHeader(msg);
            ByteBuf bodyBuffer = msg.readRetainedSlice(msg.readableBytes());
            out.add(new TransportInboundFrame(header, bodyBuffer));
        } catch (RuntimeException exception) {
            log.warn(
                    "Failed to decode request header channelId={} readableBytes={}",
                    ctx.channel().id().asShortText(),
                    msg.readableBytes(),
                    exception);
            throw exception;
        }
    }
}
