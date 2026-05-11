package io.github.stellhub.stellflow.network.transport;

import io.github.stellhub.stellflow.network.protocol.ProtocolCodecRegistry;
import io.github.stellhub.stellflow.network.protocol.RequestBody;
import io.github.stellhub.stellflow.network.protocol.RequestMessage;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import lombok.RequiredArgsConstructor;

/**
 * 请求体解码器。
 */
@Slf4j
@RequiredArgsConstructor
public class RequestBodyDecoder extends MessageToMessageDecoder<TransportInboundFrame> {

    private final ProtocolCodecRegistry protocolCodecRegistry;

    /**
     * 根据 apiKey 与 apiVersion 解析请求体。
     */
    @Override
    protected void decode(ChannelHandlerContext ctx, TransportInboundFrame msg, List<Object> out) {
        try {
            RequestBody body = protocolCodecRegistry.decodeRequestBody(msg.header(), msg.bodyBuffer());
            out.add(new RequestMessage(msg.header(), body));
        } catch (RuntimeException exception) {
            log.warn(
                    "Failed to decode request body channelId={} apiKey={} apiVersion={}",
                    ctx.channel().id().asShortText(),
                    msg.header().apiKey(),
                    msg.header().apiVersion(),
                    exception);
            throw exception;
        } finally {
            msg.bodyBuffer().release();
        }
    }
}
