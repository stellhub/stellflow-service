package io.github.stellhub.stellflow.network.protocol;

import io.netty.buffer.ByteBuf;

/**
 * JoinGroup 响应体编解码器。
 */
public class JoinGroupResponseBodyCodec implements ResponseBodyCodec<JoinGroupResponseBody> {

    @Override
    public ApiKey apiKey() {
        return ApiKey.JOIN_GROUP;
    }

    @Override
    public short apiVersion() {
        return 0;
    }

    @Override
    public Class<JoinGroupResponseBody> bodyType() {
        return JoinGroupResponseBody.class;
    }

    @Override
    public void encode(JoinGroupResponseBody body, ByteBuf buffer) {
        buffer.writeShort(body.errorCode().code());
        buffer.writeInt(body.generationId());
        ProtocolSerde.writeNullableString(buffer, body.memberId());
        ProtocolSerde.writeNullableString(buffer, body.leaderId());
    }
}
