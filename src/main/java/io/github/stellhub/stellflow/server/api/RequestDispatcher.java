package io.github.stellhub.stellflow.server.api;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import lombok.RequiredArgsConstructor;
import org.slf4j.MDC;

/**
 * 请求分发执行器。
 */
@Slf4j
@RequiredArgsConstructor
public class RequestDispatcher implements AutoCloseable {

    private final RequestChannel requestChannel;
    private final BrokerApis brokerApis;
    private final int workerCount;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private ExecutorService executorService;

    /**
     * 启动请求处理线程池。
     */
    public synchronized void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        log.info("Starting RequestDispatcher workerCount={}", workerCount);
        executorService =
                Executors.newFixedThreadPool(
                        workerCount,
                        runnable -> {
                            Thread thread = new Thread(runnable);
                            thread.setName("stellflow-request-handler");
                            thread.setDaemon(true);
                            return thread;
                        });
        for (int index = 0; index < workerCount; index++) {
            executorService.submit(this::runLoop);
        }
        log.info("RequestDispatcher started");
    }

    /**
     * 关闭请求处理线程池。
     */
    @Override
    public synchronized void close() {
        running.set(false);
        if (executorService != null) {
            executorService.shutdownNow();
        }
        log.info("RequestDispatcher closed");
    }

    private void runLoop() {
        while (running.get()) {
            try {
                RequestContext requestContext = requestChannel.takeRequest();
                bindRequestMdc(requestContext);
                try {
                    ResponseContext responseContext = brokerApis.handleRequest(requestContext);
                    requestChannel.sendResponse(responseContext);
                } finally {
                    MDC.clear();
                }
            } catch (InterruptedException interruptedException) {
                Thread.currentThread().interrupt();
                log.info("RequestDispatcher worker interrupted, exiting run loop");
                return;
            } catch (RuntimeException exception) {
                log.error("Unexpected RequestDispatcher failure", exception);
            }
        }
    }

    /**
     * 绑定请求级 MDC 上下文，便于本地日志检索。
     */
    private void bindRequestMdc(RequestContext requestContext) {
        putIfNotNull("correlationId", Integer.toString(requestContext.getCorrelationId()));
        putIfNotNull("clientId", requestContext.getClientId());
        putIfNotNull("traceId", requestContext.getTraceId());
        putIfNotNull("tenantId", requestContext.getTenantId());
        putIfNotNull("trafficClass", Byte.toString(requestContext.getTrafficClass()));
        putIfNotNull("trafficTag", requestContext.getTrafficTag());
        putIfNotNull("apiKey", requestContext.getApiKey().name());
        putIfNotNull("apiVersion", Short.toString(requestContext.getApiVersion()));
        putIfNotNull("connectionId", requestContext.getConnectionId());
    }

    /**
     * 仅在值非空时写入 MDC。
     */
    private void putIfNotNull(String key, String value) {
        if (value != null) {
            MDC.put(key, value);
        }
    }
}
