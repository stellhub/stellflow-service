package io.github.stellhub.stellflow.network.protocol;

import io.netty.buffer.ByteBuf;

/**
 * FindCoordinator 请求体编解码器。
 */
public class FindCoordinatorRequestBodyCodec implements RequestBodyCodec<FindCoordinatorRequestBody> {

    @Override
    public ApiKey apiKey() {
        return ApiKey.FIND_COORDINATOR;
    }

    @Override
    public short apiVersion() {
        return 0;
    }

    @Override
    public FindCoordinatorRequestBody decode(ByteBuf buffer) {
        return new FindCoordinatorRequestBody(
                ProtocolSerde.readNullableString(buffer), buffer.readByte());
    }
}
