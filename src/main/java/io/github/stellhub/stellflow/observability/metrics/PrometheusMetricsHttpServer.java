package io.github.stellhub.stellflow.observability.metrics;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import lombok.extern.slf4j.Slf4j;

/**
 * Prometheus 指标 HTTP 暴露端口。
 */
@Slf4j
public class PrometheusMetricsHttpServer implements AutoCloseable {

    private final MetricsHttpConfig config;
    private final ReplicaFetchMetrics replicaFetchMetrics;
    private final StellflowMetrics stellflowMetrics;
    private HttpServer server;

    public PrometheusMetricsHttpServer(
            MetricsHttpConfig config, ReplicaFetchMetrics replicaFetchMetrics) {
        this(config, replicaFetchMetrics, StellflowMetrics.global());
    }

    public PrometheusMetricsHttpServer(
            MetricsHttpConfig config,
            ReplicaFetchMetrics replicaFetchMetrics,
            StellflowMetrics stellflowMetrics) {
        this.config = config;
        this.replicaFetchMetrics = replicaFetchMetrics;
        this.stellflowMetrics = stellflowMetrics;
    }

    /**
     * 启动指标端口。
     */
    public synchronized void start() {
        if (!config.isEnabled() || server != null) {
            return;
        }
        try {
            server = HttpServer.create(new InetSocketAddress(config.getHost(), config.getPort()), 0);
            server.createContext(
                    config.getPath(),
                    exchange -> {
                        byte[] body =
                                (stellflowMetrics.renderPrometheus()
                                                + replicaFetchMetrics.renderPrometheus())
                                        .getBytes(StandardCharsets.UTF_8);
                        exchange.getResponseHeaders()
                                .set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
                        exchange.sendResponseHeaders(200, body.length);
                        try (OutputStream outputStream = exchange.getResponseBody()) {
                            outputStream.write(body);
                        }
                    });
            server.setExecutor(null);
            server.start();
            log.info(
                    "Prometheus metrics HTTP server started on {}:{}{}",
                    config.getHost(),
                    config.getPort(),
                    config.getPath());
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to start metrics HTTP server", exception);
        }
    }

    @Override
    public synchronized void close() {
        if (server == null) {
            return;
        }
        server.stop(0);
        server = null;
        log.info("Prometheus metrics HTTP server stopped");
    }
}
