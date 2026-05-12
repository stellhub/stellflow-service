package io.github.stellhub.stellflow.network.protocol;

import io.netty.buffer.ByteBuf;

/**
 * JoinGroup 请求体编解码器。
 */
public class JoinGroupRequestBodyCodec implements RequestBodyCodec<JoinGroupRequestBody> {

    @Override
    public ApiKey apiKey() {
        return ApiKey.JOIN_GROUP;
    }

    @Override
    public short apiVersion() {
        return 0;
    }

    @Override
    public JoinGroupRequestBody decode(ByteBuf buffer) {
        return new JoinGroupRequestBody(
                ProtocolSerde.readNullableString(buffer),
                ProtocolSerde.readNullableString(buffer),
                buffer.readInt());
    }
}
