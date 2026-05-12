package io.github.stellhub.stellflow.network.protocol;

import io.netty.buffer.ByteBuf;

/**
 * 集群状态响应体编解码器。
 */
public class ClusterStatusResponseBodyCodec implements ResponseBodyCodec<ClusterStatusResponseBody> {

    private final ApiKey apiKey;

    public ClusterStatusResponseBodyCodec(ApiKey apiKey) {
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
    public Class<ClusterStatusResponseBody> bodyType() {
        return ClusterStatusResponseBody.class;
    }

    @Override
    public void encode(ClusterStatusResponseBody body, ByteBuf buffer) {
        buffer.writeShort(body.errorCode().code());
        ProtocolSerde.writeNullableString(buffer, body.clusterId());
        buffer.writeInt(body.brokerCount());
        buffer.writeInt(body.topicCount());
        buffer.writeInt(body.partitionCount());
    }
}
