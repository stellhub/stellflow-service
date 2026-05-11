package io.github.stellhub.stellflow.network.transport;

import io.github.stellhub.stellflow.network.protocol.ProtocolCodecRegistry;
import io.github.stellhub.stellflow.server.api.RequestChannel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.flush.FlushConsolidationHandler;
import io.netty.handler.timeout.IdleStateHandler;
import java.util.concurrent.TimeUnit;
import lombok.RequiredArgsConstructor;

/**
 * Broker 服务端 pipeline 初始化器。
 */
@RequiredArgsConstructor
public class ServerChannelInitializer extends ChannelInitializer<SocketChannel> {

    private final NettyTransportConfig transportConfig;
    private final ProtocolCodecRegistry protocolCodecRegistry;
    private final RequestChannel requestChannel;

    /**
     * 初始化 Netty ChannelPipeline。
     */
    @Override
    protected void initChannel(SocketChannel ch) {
        ch.pipeline()
                .addLast("connectionLifecycleHandler", new ConnectionLifecycleHandler())
                .addLast("idleStateHandler", new IdleStateHandler(60, 0, 0, TimeUnit.SECONDS))
                .addLast("frameLengthDecoder", new FrameLengthDecoder(transportConfig.getMaxFrameLength()))
                .addLast("requestHeaderDecoder", new RequestHeaderDecoder())
                .addLast("requestBodyDecoder", new RequestBodyDecoder(protocolCodecRegistry))
                .addLast("requestDispatchHandler", new RequestDispatchHandler(requestChannel, "PLAINTEXT"))
                .addLast("responseEncoder", new ResponseEncoder(protocolCodecRegistry))
                .addLast("flushConsolidationHandler", new FlushConsolidationHandler(256, true))
                .addLast("pipelineExceptionHandler", new PipelineExceptionHandler());
    }
}
