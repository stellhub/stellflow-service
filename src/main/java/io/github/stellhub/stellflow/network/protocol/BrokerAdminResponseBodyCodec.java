package io.github.stellhub.stellflow.network.protocol;

import io.netty.buffer.ByteBuf;

/**
 * Broker 管理响应体编解码器。
 */
public class BrokerAdminResponseBodyCodec implements ResponseBodyCodec<BrokerAdminResponseBody> {

    @Override
    public ApiKey apiKey() {
        return ApiKey.DECOMMISSION_BROKER;
    }

    @Override
    public short apiVersion() {
        return 0;
    }

    @Override
    public Class<BrokerAdminResponseBody> bodyType() {
        return BrokerAdminResponseBody.class;
    }

    @Override
    public void encode(BrokerAdminResponseBody body, ByteBuf buffer) {
        buffer.writeShort(body.errorCode().code());
        ProtocolSerde.writeNullableString(buffer, body.message());
    }
}
