package io.github.stellhub.stellflow.network.transport;

import io.github.stellhub.stellflow.network.protocol.RequestMessage;
import io.github.stellhub.stellflow.server.api.RequestChannel;
import io.github.stellhub.stellflow.server.api.RequestContext;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;
import lombok.RequiredArgsConstructor;

/**
 * 请求投递处理器。
 */
@Slf4j
@RequiredArgsConstructor
public class RequestDispatchHandler extends SimpleChannelInboundHandler<RequestMessage> {

    private final RequestChannel requestChannel;
    private final String listenerName;

    /**
     * 将请求投递到 RequestChannel。
     */
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RequestMessage msg) {
        RequestContext requestContext =
                RequestContext.builder()
                        .connectionId(ctx.channel().id().asShortText())
                        .clientId(msg.header().clientId())
                        .traceId(msg.header().traceId())
                        .spanId(msg.header().spanId())
                        .traceFlags(msg.header().traceFlags())
                        .tenantId(msg.header().tenantId())
                        .quotaKey(msg.header().quotaKey())
                        .authContextId(msg.header().authContextId())
                        .trafficClass(msg.header().trafficClass())
                        .trafficTag(msg.header().trafficTag())
                        .listenerName(listenerName)
                        .apiKey(msg.header().apiKey())
                        .apiVersion(msg.header().apiVersion())
                        .correlationId(msg.header().correlationId())
                        .requestHeader(msg.header())
                        .requestBody(msg.body())
                        .receivedTimeMs(System.currentTimeMillis())
                        .responseWriter(new NettyResponseWriter(ctx.channel()))
                        .build();
        log.debug(
                "Dispatching request channelId={} apiKey={} apiVersion={} correlationId={} clientId={}",
                requestContext.getConnectionId(),
                requestContext.getApiKey(),
                requestContext.getApiVersion(),
                requestContext.getCorrelationId(),
                requestContext.getClientId());
        requestChannel.sendRequest(requestContext);
    }
}
