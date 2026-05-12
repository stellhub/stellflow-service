package io.github.stellhub.stellflow.network.protocol;

import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.List;

/**
 * OffsetCommit 请求体编解码器。
 */
public class OffsetCommitRequestBodyCodec implements RequestBodyCodec<OffsetCommitRequestBody> {

    @Override
    public ApiKey apiKey() {
        return ApiKey.OFFSET_COMMIT;
    }

    @Override
    public short apiVersion() {
        return 0;
    }

    @Override
    public OffsetCommitRequestBody decode(ByteBuf buffer) {
        String groupId = ProtocolSerde.readNullableString(buffer);
        int topicCount = buffer.readInt();
        List<OffsetCommitTopic> topics = new ArrayList<>(Math.max(topicCount, 0));
        for (int topicIndex = 0; topicIndex < topicCount; topicIndex++) {
            String topic = ProtocolSerde.readNullableString(buffer);
            int partitionCount = buffer.readInt();
            List<OffsetCommitPartition> partitions = new ArrayList<>(Math.max(partitionCount, 0));
            for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++) {
                partitions.add(
                        new OffsetCommitPartition(
                                buffer.readInt(), buffer.readLong(), ProtocolSerde.readNullableString(buffer)));
            }
            topics.add(new OffsetCommitTopic(topic, partitions));
        }
        return new OffsetCommitRequestBody(groupId, topics);
    }
}
