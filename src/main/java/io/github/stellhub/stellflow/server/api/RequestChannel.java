package io.github.stellhub.stellflow.server.api;

/**
 * 网络层与业务层之间的请求通道。
 */
public interface RequestChannel {

    /**
     * 投递请求。
     */
    void sendRequest(RequestContext requestContext);

    /**
     * 获取一个待处理请求。
     */
    RequestContext takeRequest() throws InterruptedException;

    /**
     * 投递响应。
     */
    void sendResponse(ResponseContext responseContext);

    /**
     * 获取一个待回写响应。
     */
    ResponseContext takeResponse() throws InterruptedException;
}
