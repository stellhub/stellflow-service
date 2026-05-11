package io.github.stellhub.stellflow.network.transport;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;

/**
 * 连接生命周期处理器。
 */
@Slf4j
public class ConnectionLifecycleHandler extends ChannelInboundHandlerAdapter {

    /**
     * 在连接建立时继续传播事件。
     */
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        log.info(
                "Connection active channelId={} remoteAddress={}",
                ctx.channel().id().asShortText(),
                ctx.channel().remoteAddress());
        super.channelActive(ctx);
    }

    /**
     * 在连接关闭时继续传播事件。
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        log.info(
                "Connection inactive channelId={} remoteAddress={}",
                ctx.channel().id().asShortText(),
                ctx.channel().remoteAddress());
        super.channelInactive(ctx);
    }

    /**
     * 记录连接级异常并关闭连接。
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn(
                "Connection exception channelId={} remoteAddress={}",
                ctx.channel().id().asShortText(),
                ctx.channel().remoteAddress(),
                cause);
        ctx.close();
    }
}
