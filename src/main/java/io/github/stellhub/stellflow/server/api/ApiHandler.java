package io.github.stellhub.stellflow.server.api;

import io.github.stellhub.stellflow.network.protocol.ApiKey;

/**
 * API 处理器接口。
 */
public interface ApiHandler {

    /**
     * 返回当前处理器负责的 API。
     */
    ApiKey apiKey();

    /**
     * 处理请求并生成响应。
     */
    ResponseContext handle(RequestContext requestContext);
}
