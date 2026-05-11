package io.github.stellhub.stellflow.server.api;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import lombok.RequiredArgsConstructor;

/**
 * 响应回写执行器。
 */
@Slf4j
@RequiredArgsConstructor
public class ResponseResponder implements AutoCloseable {

    private final RequestChannel requestChannel;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private ExecutorService executorService;

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
}
