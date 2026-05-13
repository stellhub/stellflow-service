package io.github.stellhub.stellflow.network.protocol;

import io.netty.buffer.ByteBuf;

/**
 * 事务控制响应体编解码器。
 */
public class TransactionResponseBodyCodec implements ResponseBodyCodec<TransactionResponseBody> {

    private final ApiKey apiKey;

    public TransactionResponseBodyCodec(ApiKey apiKey) {
        this.apiKey = apiKey;
    }

    @Override
    public ApiKey apiKey() {
        return apiKey;
    }

    @Override
    public short apiVersion() {
        return 0;
    }

    @Override
    public Class<TransactionResponseBody> bodyType() {
        return TransactionResponseBody.class;
    }

    @Override
    public void encode(TransactionResponseBody body, ByteBuf buffer) {
        buffer.writeShort(body.errorCode().code());
        buffer.writeLong(body.producerId());
        buffer.writeShort(body.producerEpoch());
        ProtocolSerde.writeNullableString(buffer, body.transactionState());
    }
}
