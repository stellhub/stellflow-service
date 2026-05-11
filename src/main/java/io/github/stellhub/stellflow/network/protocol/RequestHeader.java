package io.github.stellhub.stellflow.network.protocol;

/**
 * 统一请求头。
 */
public record RequestHeader(
        short apiKeyCode,
        short apiVersion,
        short headerVersion,
        int correlationId,
        String clientId,
        String traceId,
        String spanId,
        byte traceFlags,
        String tenantId,
        String quotaKey,
        String authContextId,
        byte trafficClass,
        String trafficTag,
        short flags) {

    /**
     * 返回请求头对应的 API 标识。
     */
    public ApiKey apiKey() {
        return ApiKey.fromCode(apiKeyCode);
    }
}
