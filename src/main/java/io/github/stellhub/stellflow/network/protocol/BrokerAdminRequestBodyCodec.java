package io.github.stellhub.stellflow.network.protocol;

import io.netty.buffer.ByteBuf;

/**
 * Broker 管理请求体编解码器。
 */
public class BrokerAdminRequestBodyCodec implements RequestBodyCodec<BrokerAdminRequestBody> {

    @Override
    public ApiKey apiKey() {
        return ApiKey.DECOMMISSION_BROKER;
    }

    @Override
    public short apiVersion() {
        return 0;
    }

    @Override
    public BrokerAdminRequestBody decode(ByteBuf buffer) {
        return new BrokerAdminRequestBody(buffer.readInt());
    }
}
