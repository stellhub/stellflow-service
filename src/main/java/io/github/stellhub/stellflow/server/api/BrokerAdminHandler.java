package io.github.stellhub.stellflow.server.api;

import io.github.stellhub.stellflow.network.protocol.ApiKey;
import io.github.stellhub.stellflow.network.protocol.BrokerAdminRequestBody;
import io.github.stellhub.stellflow.network.protocol.BrokerAdminResponseBody;
import io.github.stellhub.stellflow.network.protocol.ErrorCode;
import io.github.stellhub.stellflow.network.protocol.ResponseHeader;

/**
 * Broker 运维请求处理器。
 */
public class BrokerAdminHandler implements ApiHandler {

    @Override
    public ApiKey apiKey() {
        return ApiKey.DECOMMISSION_BROKER;
    }

    @Override
    public ResponseContext handle(RequestContext requestContext) {
        BrokerAdminRequestBody body = (BrokerAdminRequestBody) requestContext.getRequestBody();
        BrokerAdminResponseBody responseBody =
                new BrokerAdminResponseBody(
                        ErrorCode.NONE,
                        "decommission plan accepted for brokerId=" + body.brokerId());
        return ResponseContext.builder()
                .requestContext(requestContext)
                .apiKey(ApiKey.DECOMMISSION_BROKER)
                .apiVersion((short) 0)
                .responseHeader(new ResponseHeader(requestContext.getCorrelationId(), (short) 2, ErrorCode.NONE, 0))
                .responseBody(responseBody)
                .build();
    }
}
