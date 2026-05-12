package io.github.stellhub.stellflow.server.api;

import io.github.stellhub.stellflow.coordinator.OffsetStore;
import io.github.stellhub.stellflow.network.protocol.ApiKey;
import io.github.stellhub.stellflow.network.protocol.ErrorCode;
import io.github.stellhub.stellflow.network.protocol.OffsetFetchPartitionResponse;
import io.github.stellhub.stellflow.network.protocol.OffsetFetchRequestBody;
import io.github.stellhub.stellflow.network.protocol.OffsetFetchResponseBody;
import io.github.stellhub.stellflow.network.protocol.OffsetFetchTopicResponse;
import io.github.stellhub.stellflow.network.protocol.ResponseHeader;
import java.util.ArrayList;
import java.util.List;

/**
 * OffsetFetch 请求处理器。
 */
public class OffsetFetchHandler implements ApiHandler {

    private final OffsetStore offsetStore;

    public OffsetFetchHandler(OffsetStore offsetStore) {
        this.offsetStore = offsetStore;
    }

    @Override
    public ApiKey apiKey() {
        return ApiKey.OFFSET_FETCH;
    }

    @Override
    public ResponseContext handle(RequestContext requestContext) {
        OffsetFetchRequestBody body = (OffsetFetchRequestBody) requestContext.getRequestBody();
        List<OffsetFetchTopicResponse> topicResponses = new ArrayList<>();
        for (var topic : body.topics()) {
            List<OffsetFetchPartitionResponse> partitionResponses = new ArrayList<>();
            for (var partition : topic.partitions()) {
                var offset = offsetStore.fetch(body.groupId(), topic.topic(), partition.partition());
                partitionResponses.add(
                        offset.map(
                                        value ->
                                                new OffsetFetchPartitionResponse(
                                                        partition.partition(),
                                                        value.offset(),
                                                        value.metadata(),
                                                        ErrorCode.NONE))
                                .orElseGet(
                                        () ->
                                                new OffsetFetchPartitionResponse(
                                                        partition.partition(),
                                                        -1,
                                                        "",
                                                        ErrorCode.NONE)));
            }
            topicResponses.add(new OffsetFetchTopicResponse(topic.topic(), partitionResponses));
        }
        return ResponseContext.builder()
                .requestContext(requestContext)
                .apiKey(ApiKey.OFFSET_FETCH)
                .apiVersion((short) 0)
                .responseHeader(new ResponseHeader(requestContext.getCorrelationId(), (short) 2, ErrorCode.NONE, 0))
                .responseBody(new OffsetFetchResponseBody(topicResponses))
                .build();
    }
}
