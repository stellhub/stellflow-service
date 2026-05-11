package io.github.stellhub.stellflow.network.protocol;

import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.List;

/**
 * Metadata 请求体编解码器。
 */
public class MetadataRequestBodyCodec implements RequestBodyCodec<MetadataRequestBody> {

    @Override
    public ApiKey apiKey() {
        return ApiKey.METADATA;
    }

    @Override
    public short apiVersion() {
        return 0;
    }

    @Override
    public MetadataRequestBody decode(ByteBuf buffer) {
        int length = buffer.readInt();
        List<MetadataTopicRequest> topics = new ArrayList<>(Math.max(length, 0));
        for (int index = 0; index < length; index++) {
            topics.add(new MetadataTopicRequest(ProtocolSerde.readNullableString(buffer)));
        }
        return new MetadataRequestBody(
                topics,
                buffer.readByte() == 1,
                buffer.readByte() == 1,
                buffer.readByte() == 1);
    }
}
