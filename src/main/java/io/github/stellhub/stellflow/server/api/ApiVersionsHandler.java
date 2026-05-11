package io.github.stellhub.stellflow.server.api;

import io.github.stellhub.stellflow.network.protocol.ApiKey;
import io.github.stellhub.stellflow.network.protocol.ApiVersionRange;
import io.github.stellhub.stellflow.network.protocol.ApiVersionsResponseBody;
import io.github.stellhub.stellflow.network.protocol.EmptyResponseBody;
import io.github.stellhub.stellflow.network.protocol.ErrorCode;
import io.github.stellhub.stellflow.network.protocol.ResponseHeader;
import java.util.Arrays;
import java.util.List;

/**
 * ApiVersions 请求处理器。
 */
public class ApiVersionsHandler implements ApiHandler {

    /**
     * 返回当前处理器负责的 API。
     */
    @Override
    public ApiKey apiKey() {
        return ApiKey.API_VERSIONS;
    }

    /**
     * 生成 ApiVersions 响应。
     */
    @Override
    public ResponseContext handle(RequestContext requestContext) {
        if (requestContext.getApiVersion() != 0) {
            return ResponseContext.builder()
                    .requestContext(requestContext)
                    .apiKey(ApiKey.API_VERSIONS)
                    .apiVersion((short) 0)
                    .responseHeader(
                            new ResponseHeader(
                                    requestContext.getCorrelationId(),
                                    (short) 2,
                                    ErrorCode.UNSUPPORTED_VERSION,
                                    0))
                    .responseBody(EmptyResponseBody.INSTANCE)
                    .build();
        }

        List<ApiVersionRange> apiVersions =
                Arrays.stream(ApiKey.values())
                        .filter(apiKey -> apiKey != ApiKey.UNKNOWN)
                        .map(apiKey -> new ApiVersionRange(apiKey, (short) 0, (short) 0))
                        .toList();

        ApiVersionsResponseBody responseBody =
                new ApiVersionsResponseBody(
                        apiVersions,
                        "stellflow-broker",
                        "0.0.1-SNAPSHOT",
                        List.of("fetch.long_poll", "compression.none"));

        return ResponseContext.builder()
                .requestContext(requestContext)
                .apiKey(ApiKey.API_VERSIONS)
                .apiVersion((short) 0)
                .responseHeader(
                    new ResponseHeader(
                            requestContext.getCorrelationId(), (short) 2, ErrorCode.NONE, 0))
                .responseBody(responseBody)
                .build();
    }
}
