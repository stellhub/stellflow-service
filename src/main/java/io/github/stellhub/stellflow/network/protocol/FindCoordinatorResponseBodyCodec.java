package io.github.stellhub.stellflow.network.protocol;

import io.netty.buffer.ByteBuf;

/**
 * FindCoordinator 响应体编解码器。
 */
public class FindCoordinatorResponseBodyCodec implements ResponseBodyCodec<FindCoordinatorResponseBody> {

    @Override
    public ApiKey apiKey() {
        return ApiKey.FIND_COORDINATOR;
    }

    @Override
    public short apiVersion() {
        return 0;
    }

    @Override
    public Class<FindCoordinatorResponseBody> bodyType() {
        return FindCoordinatorResponseBody.class;
    }

    @Override
    public void encode(FindCoordinatorResponseBody body, ByteBuf buffer) {
        buffer.writeShort(body.errorCode().code());
        buffer.writeInt(body.nodeId());
        ProtocolSerde.writeNullableString(buffer, body.host());
        buffer.writeInt(body.port());
    }
}
