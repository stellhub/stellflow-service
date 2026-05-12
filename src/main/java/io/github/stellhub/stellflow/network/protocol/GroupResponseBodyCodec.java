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
        ErrorCode errorCode =
                body instanceof HeartbeatResponseBody heartbeat
                        ? heartbeat.errorCode()
                        : ((SyncGroupResponseBody) body).errorCode();
        buffer.writeShort(errorCode.code());
    }
}
