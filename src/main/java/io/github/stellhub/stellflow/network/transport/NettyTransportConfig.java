package io.github.stellhub.stellflow.network.transport;

import java.util.Properties;
import io.github.stellhub.stellflow.config.StellflowConfigLoader;
import lombok.Builder;
import lombok.Getter;

/**
 * Netty 传输层配置。
 */
@Builder
@Getter
public class NettyTransportConfig {

    private static final String PREFIX = "stellflow.network.transport.";

    @Builder.Default private final String host = "0.0.0.0";
    @Builder.Default private final int port = 9092;
    @Builder.Default private final int bossThreads = 1;
    @Builder.Default
    private final int workerThreads = Math.max(2, Runtime.getRuntime().availableProcessors() / 2);
    @Builder.Default private final int soRcvBuf = 4 * 1024 * 1024;
    @Builder.Default private final int soSndBuf = 4 * 1024 * 1024;
    @Builder.Default private final int maxFrameLength = 64 * 1024 * 1024;

    /**
     * 从配置文件和系统属性加载传输层配置。
     */
    public static NettyTransportConfig load() {
        NettyTransportConfig defaults = NettyTransportConfig.builder().build();
        Properties properties = StellflowConfigLoader.load();

        return NettyTransportConfig.builder()
                .host(readString(properties, "host", defaults.getHost()))
                .port(readPositiveInt(properties, "port", defaults.getPort()))
                .bossThreads(readPositiveInt(properties, "bossThreads", defaults.getBossThreads()))
                .workerThreads(
                        readPositiveInt(properties, "workerThreads", defaults.getWorkerThreads()))
                .soRcvBuf(readPositiveInt(properties, "soRcvBuf", defaults.getSoRcvBuf()))
                .soSndBuf(readPositiveInt(properties, "soSndBuf", defaults.getSoSndBuf()))
                .maxFrameLength(
                        readPositiveInt(properties, "maxFrameLength", defaults.getMaxFrameLength()))
                .build();
    }

    /**
     * 读取字符串配置。
     */
    private static String readString(Properties properties, String key, String defaultValue) {
        return StellflowConfigLoader.readString(properties, PREFIX + key, defaultValue);
    }

    /**
     * 读取正整数配置。
     */
    private static int readPositiveInt(Properties properties, String key, int defaultValue) {
        return StellflowConfigLoader.readPositiveInt(properties, PREFIX + key, defaultValue);
    }
}
