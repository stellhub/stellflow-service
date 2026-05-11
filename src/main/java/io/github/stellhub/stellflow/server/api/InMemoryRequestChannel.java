package io.github.stellhub.stellflow.server.api;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 基于内存队列的 RequestChannel。
 */
public class InMemoryRequestChannel implements RequestChannel {

    private final BlockingQueue<RequestContext> requestQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<ResponseContext> responseQueue = new LinkedBlockingQueue<>();

    /**
     * 投递请求。
     */
    @Override
    public void sendRequest(RequestContext requestContext) {
        requestQueue.offer(requestContext);
    }

    /**
     * 获取待处理请求。
     */
    @Override
    public RequestContext takeRequest() throws InterruptedException {
        return requestQueue.take();
    }

    /**
     * 投递响应。
     */
    @Override
    public void sendResponse(ResponseContext responseContext) {
        responseQueue.offer(responseContext);
    }

    /**
     * 获取待回写响应。
     */
    @Override
    public ResponseContext takeResponse() throws InterruptedException {
        return responseQueue.take();
    }
}
