package io.github.stellhub.stellflow.network.protocol;

import io.netty.buffer.ByteBuf;

/**
 * 简单 group 请求体编解码器。
 */
public class GroupRequestBodyCodec implements RequestBodyCodec<RequestBody> {

    private final ApiKey apiKey;

    public GroupRequestBodyCodec(ApiKey apiKey) {
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
    public RequestBody decode(ByteBuf buffer) {
        String groupId = ProtocolSerde.readNullableString(buffer);
        int generationId = buffer.readInt();
        String memberId = ProtocolSerde.readNullableString(buffer);
        if (apiKey == ApiKey.HEARTBEAT) {
            return new HeartbeatRequestBody(groupId, generationId, memberId);
        }
        return new SyncGroupRequestBody(groupId, generationId, memberId);
    }
}
