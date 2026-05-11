package io.github.stellhub.stellflow.server.api;

import io.github.stellhub.stellflow.network.protocol.ApiKey;
import io.github.stellhub.stellflow.network.protocol.RequestBody;
import io.github.stellhub.stellflow.network.protocol.RequestHeader;
import lombok.Builder;
import lombok.Getter;

/**
 * 请求上下文。
 */
@Builder
@Getter
public class RequestContext {
    private final String connectionId;
    private final String clientId;
    private final String traceId;
    private final String spanId;
    private final byte traceFlags;
    private final String tenantId;
    private final String quotaKey;
    private final String authContextId;
    private final byte trafficClass;
    private final String trafficTag;
    private final String listenerName;
    private final ApiKey apiKey;
    private final short apiVersion;
    private final int correlationId;
    private final RequestHeader requestHeader;
    private final RequestBody requestBody;
    private final long receivedTimeMs;
    private final ResponseWriter responseWriter;
}
