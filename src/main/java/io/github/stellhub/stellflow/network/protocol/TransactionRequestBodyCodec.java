package io.github.stellhub.stellflow.network.protocol;

import io.netty.buffer.ByteBuf;

/**
 * 事务控制请求体编解码器。
 */
public class TransactionRequestBodyCodec implements RequestBodyCodec<TransactionRequestBody> {

    private final ApiKey apiKey;

    public TransactionRequestBodyCodec(ApiKey apiKey) {
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
    public TransactionRequestBody decode(ByteBuf buffer) {
        String transactionalId = ProtocolSerde.readNullableString(buffer);
        long producerId = buffer.readLong();
        short producerEpoch = buffer.readShort();
        boolean commit = apiKey == ApiKey.END_TRANSACTION && buffer.readBoolean();
        return new TransactionRequestBody(transactionalId, producerId, producerEpoch, commit);
    }
}
