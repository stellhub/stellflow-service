package io.github.stellhub.stellflow.network.protocol;

import io.netty.buffer.ByteBuf;

/**
 * Metadata 响应体编解码器。
 */
public class MetadataResponseBodyCodec implements ResponseBodyCodec<MetadataResponseBody> {

    @Override
    public ApiKey apiKey() {
        return ApiKey.METADATA;
    }

    @Override
    public short apiVersion() {
        return 0;
    }

    @Override
    public Class<MetadataResponseBody> bodyType() {
        return MetadataResponseBody.class;
    }

    @Override
    public void encode(MetadataResponseBody body, ByteBuf buffer) {
        ProtocolSerde.writeNullableString(buffer, body.clusterId());
        buffer.writeInt(body.controllerId());

        buffer.writeInt(body.brokers().size());
        for (MetadataBroker broker : body.brokers()) {
            buffer.writeInt(broker.brokerId());
            ProtocolSerde.writeNullableString(buffer, broker.host());
            buffer.writeInt(broker.port());
            ProtocolSerde.writeNullableString(buffer, broker.rack());
        }

        buffer.writeInt(body.topics().size());
        for (MetadataTopicResponse topic : body.topics()) {
            buffer.writeShort(topic.errorCode().code());
            ProtocolSerde.writeNullableString(buffer, topic.topic());
            buffer.writeByte(topic.internal() ? 1 : 0);

            buffer.writeInt(topic.partitions().size());
            for (MetadataPartitionResponse partition : topic.partitions()) {
                buffer.writeShort(partition.errorCode().code());
                buffer.writeInt(partition.partition());
                buffer.writeInt(partition.leaderId());
                buffer.writeInt(partition.leaderEpoch());
                ProtocolSerde.writeIntArray(buffer, partition.replicaNodes());
                ProtocolSerde.writeIntArray(buffer, partition.isrNodes());
                ProtocolSerde.writeIntArray(buffer, partition.offlineReplicaNodes());
            }

            buffer.writeInt(topic.topicAuthorizedOperations());
        }

        buffer.writeInt(body.clusterAuthorizedOperations());
    }
}
