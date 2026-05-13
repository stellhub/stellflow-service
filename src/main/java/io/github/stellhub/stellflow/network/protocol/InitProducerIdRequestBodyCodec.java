package io.github.stellhub.stellflow.network.protocol;

import io.netty.buffer.ByteBuf;

/**
 * InitProducerId 请求体编解码器。
 */
public class InitProducerIdRequestBodyCodec implements RequestBodyCodec<InitProducerIdRequestBody> {

    @Override
    public ApiKey apiKey() {
        return ApiKey.INIT_PRODUCER_ID;
    }

    @Override
    public short apiVersion() {
        return 0;
    }

    @Override
    public InitProducerIdRequestBody decode(ByteBuf buffer) {
        return new InitProducerIdRequestBody(ProtocolSerde.readNullableString(buffer));
    }
}
