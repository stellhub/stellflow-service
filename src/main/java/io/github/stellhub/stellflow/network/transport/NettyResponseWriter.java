package io.github.stellhub.stellflow.network.transport;

import io.github.stellhub.stellflow.server.api.ResponseContext;
import io.github.stellhub.stellflow.server.api.ResponseWriter;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;
import lombok.RequiredArgsConstructor;

/**
 * Netty 响应写出器。
 */
@Slf4j
@RequiredArgsConstructor
public class NettyResponseWriter implements ResponseWriter {

    private final Channel channel;

    /**
     * 将响应交给 Netty pipeline 编码并写出。
     */
    @Override
    public void write(ResponseContext responseContext) {
        channel.writeAndFlush(responseContext)
                .addListener(
                        future -> {
                            if (!future.isSuccess()) {
                                log.warn(
                                        "Failed to write response channelId={} apiKey={} apiVersion={} correlationId={}",
                                        channel.id().asShortText(),
                                        responseContext.getApiKey(),
                                        responseContext.getApiVersion(),
                                        responseContext.getRequestContext().getCorrelationId(),
                                        future.cause());
                            }
                        });
    }
}
