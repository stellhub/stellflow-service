package io.github.stellhub.stellflow.network.protocol;

import io.netty.buffer.ByteBuf;

/**
 * InitProducerId 响应体编解码器。
 */
public class InitProducerIdResponseBodyCodec
        implements ResponseBodyCodec<InitProducerIdResponseBody> {

    @Override
    public ApiKey apiKey() {
        return ApiKey.INIT_PRODUCER_ID;
    }

    @Override
    public short apiVersion() {
        return 0;
    }

    @Override
    public Class<InitProducerIdResponseBody> bodyType() {
        return InitProducerIdResponseBody.class;
    }

    @Override
    public void encode(InitProducerIdResponseBody body, ByteBuf buffer) {
        buffer.writeShort(body.errorCode().code());
        buffer.writeLong(body.producerId());
        buffer.writeShort(body.producerEpoch());
    }
}
