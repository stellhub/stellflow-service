package io.github.stellhub.stellflow.server.api;

import io.github.stellhub.stellflow.observability.logging.MessageFlowDebugLogConfig;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * 响应回写执行器。
 */
@Slf4j
@RequiredArgsConstructor
public class ResponseResponder implements AutoCloseable {

    private final RequestChannel requestChannel;
    private final MessageFlowDebugLogConfig debugLogConfig;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private ExecutorService executorService;

    public ResponseResponder(RequestChannel requestChannel) {
        this(requestChannel, MessageFlowDebugLogConfig.load());
    }

    /**
     * 启动响应回写线程。
     */
    public synchronized void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        log.info("Starting ResponseResponder");
        executorService =
                Executors.newSingleThreadExecutor(
                        runnable -> {
                            Thread thread = new Thread(runnable);
                            thread.setName("stellflow-response-responder");
                            thread.setDaemon(true);
                            return thread;
                        });
        executorService.submit(this::runLoop);
        log.info("ResponseResponder started");
    }

    /**
     * 关闭响应回写线程。
     */
    @Override
    public synchronized void close() {
        running.set(false);
        if (executorService != null) {
            executorService.shutdownNow();
        }
        log.info("ResponseResponder closed");
    }

    private void runLoop() {
        while (running.get()) {
            try {
                ResponseContext responseContext = requestChannel.takeResponse();
                logResponseDebug(responseContext);
                responseContext.getRequestContext().getResponseWriter().write(responseContext);
            } catch (InterruptedException interruptedException) {
                Thread.currentThread().interrupt();
                log.info("ResponseResponder worker interrupted, exiting run loop");
                return;
            } catch (RuntimeException exception) {
                log.error("Unexpected ResponseResponder failure", exception);
            }
        }
    }

    private void logResponseDebug(ResponseContext responseContext) {
        if (!debugLogConfig.isEnabled()) {
            return;
        }
        RequestContext requestContext = responseContext.getRequestContext();
        log.info(
                "Send response debug connectionId={} clientId={} correlationId={} traceId={} apiKey={} apiVersion={} errorCode={} zeroCopyRegions={} fetchRecordRegions={}",
                requestContext.getConnectionId(),
                requestContext.getClientId(),
                requestContext.getCorrelationId(),
                requestContext.getTraceId(),
                responseContext.getApiKey(),
                responseContext.getApiVersion(),
                responseContext.getResponseHeader() == null
                        ? null
                        : responseContext.getResponseHeader().errorCode(),
                responseContext.getZeroCopyFileRegions().size(),
                responseContext.getFetchRecordsFileRegions().size());
    }
}
