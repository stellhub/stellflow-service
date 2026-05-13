package io.github.stellhub.stellflow.network.protocol;

import io.netty.buffer.ByteBuf;

/**
 * 简单 group 响应体编解码器。
 */
public class GroupResponseBodyCodec implements ResponseBodyCodec<ResponseBody> {

    private final ApiKey apiKey;

    public GroupResponseBodyCodec(ApiKey apiKey) {
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
    public Class<ResponseBody> bodyType() {
        return ResponseBody.class;
    }

    @Override
    public void encode(ResponseBody body, ByteBuf buffer) {
        if (body instanceof HeartbeatResponseBody heartbeat) {
            buffer.writeShort(heartbeat.errorCode().code());
            return;
        }
        SyncGroupResponseBody syncGroup = (SyncGroupResponseBody) body;
        buffer.writeShort(syncGroup.errorCode().code());
        buffer.writeInt(syncGroup.assignments().size());
        for (ConsumerPartitionAssignment assignment : syncGroup.assignments()) {
            ProtocolSerde.writeNullableString(buffer, assignment.topic());
            buffer.writeInt(assignment.partition());
        }
    }
}
