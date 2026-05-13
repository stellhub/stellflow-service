package io.github.stellhub.stellflow.server.api;

import io.github.stellhub.stellflow.network.protocol.ApiKey;
import io.github.stellhub.stellflow.network.protocol.ResponseBody;
import io.github.stellhub.stellflow.network.protocol.ResponseHeader;
import java.util.List;
import lombok.Builder;
import lombok.Getter;

/**
 * 响应上下文。
 */
@Builder
@Getter
public class ResponseContext {
    private final RequestContext requestContext;
    private final ApiKey apiKey;
    private final short apiVersion;
    private final ResponseHeader responseHeader;
    private final ResponseBody responseBody;
    @Builder.Default private final List<ZeroCopyFileRegion> zeroCopyFileRegions = List.of();
}
