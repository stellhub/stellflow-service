package io.github.stellhub.stellflow.network.transport;

import io.github.stellhub.stellflow.network.protocol.ProtocolCodecRegistry;
import io.github.stellhub.stellflow.server.api.RequestChannel;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import lombok.extern.slf4j.Slf4j;
import lombok.RequiredArgsConstructor;

/**
 * Broker SocketServer 骨架。
 */
@Slf4j
@RequiredArgsConstructor
public class SocketServer implements AutoCloseable {

    private final NettyTransportConfig transportConfig;
    private final ProtocolCodecRegistry protocolCodecRegistry;
    private final RequestChannel requestChannel;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;

    /**
     * 启动 SocketServer。
     */
    public synchronized void start() throws InterruptedException {
        log.info(
                "Binding SocketServer host={} port={} bossThreads={} workerThreads={} soRcvBuf={} soSndBuf={} maxFrameLength={}",
                transportConfig.getHost(),
                transportConfig.getPort(),
                transportConfig.getBossThreads(),
                transportConfig.getWorkerThreads(),
                transportConfig.getSoRcvBuf(),
                transportConfig.getSoSndBuf(),
                transportConfig.getMaxFrameLength());
        bossGroup = new NioEventLoopGroup(transportConfig.getBossThreads());
        workerGroup = new NioEventLoopGroup(transportConfig.getWorkerThreads());

        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap
                .group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 1024)
                .childOption(ChannelOption.SO_RCVBUF, transportConfig.getSoRcvBuf())
                .childOption(ChannelOption.SO_SNDBUF, transportConfig.getSoSndBuf())
                .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .childHandler(
                        new ServerChannelInitializer(
                                transportConfig, protocolCodecRegistry, requestChannel));

        serverChannel =
                serverBootstrap.bind(transportConfig.getHost(), transportConfig.getPort()).sync().channel();
        log.info(
                "SocketServer bound successfully on {}:{}",
                transportConfig.getHost(),
                transportConfig.getPort());
    }

    /**
     * 阻塞等待服务端 channel 关闭。
     */
    public void awaitClose() throws InterruptedException {
        if (serverChannel != null) {
            serverChannel.closeFuture().sync();
        }
    }

    /**
     * 关闭 SocketServer。
     */
    @Override
    public synchronized void close() {
        log.info("Closing SocketServer");
        if (serverChannel != null) {
            serverChannel.close();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
        log.info("SocketServer close signal sent");
    }
}
