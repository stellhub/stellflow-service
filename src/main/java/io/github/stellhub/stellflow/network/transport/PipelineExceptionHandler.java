package io.github.stellhub.stellflow.network.transport;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;

/**
 * Pipeline 尾部异常收口处理器。
 */
@Slf4j
public class PipelineExceptionHandler extends ChannelInboundHandlerAdapter {

    /**
     * 记录未处理异常并关闭连接。
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn(
                "Unhandled pipeline exception channelId={} remoteAddress={}",
                ctx.channel().id().asShortText(),
                ctx.channel().remoteAddress(),
                cause);
        ctx.close();
    }
}
