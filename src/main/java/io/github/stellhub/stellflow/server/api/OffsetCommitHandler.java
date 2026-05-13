package io.github.stellhub.stellflow.server.api;

import io.github.stellhub.stellflow.coordinator.OffsetStore;
import io.github.stellhub.stellflow.network.protocol.ApiKey;
import io.github.stellhub.stellflow.network.protocol.ErrorCode;
import io.github.stellhub.stellflow.network.protocol.OffsetCommitPartitionResponse;
import io.github.stellhub.stellflow.network.protocol.OffsetCommitRequestBody;
import io.github.stellhub.stellflow.network.protocol.OffsetCommitResponseBody;
import io.github.stellhub.stellflow.network.protocol.OffsetCommitTopicResponse;
import io.github.stellhub.stellflow.observability.metrics.StellflowMetrics;
import io.github.stellhub.stellflow.network.protocol.ResponseHeader;
import java.util.ArrayList;
import java.util.List;

/**
 * OffsetCommit 请求处理器。
 */
public class OffsetCommitHandler implements ApiHandler {

    private final OffsetStore offsetStore;
    private final StellflowMetrics metrics;

    public OffsetCommitHandler(OffsetStore offsetStore) {
        this(offsetStore, StellflowMetrics.global());
    }

    public OffsetCommitHandler(OffsetStore offsetStore, StellflowMetrics metrics) {
        this.offsetStore = offsetStore;
        this.metrics = metrics;
    }

    @Override
    public ApiKey apiKey() {
        return ApiKey.OFFSET_COMMIT;
    }

    @Override
    public ResponseContext handle(RequestContext requestContext) {
        long startMs = System.currentTimeMillis();
        OffsetCommitRequestBody body = (OffsetCommitRequestBody) requestContext.getRequestBody();
        List<OffsetCommitTopicResponse> topicResponses = new ArrayList<>();
        for (var topic : body.topics()) {
            List<OffsetCommitPartitionResponse> partitionResponses = new ArrayList<>();
            for (var partition : topic.partitions()) {
                offsetStore.commit(
                        body.groupId(),
                        topic.topic(),
                        partition.partition(),
                        partition.offset(),
                        partition.metadata());
                partitionResponses.add(
                        new OffsetCommitPartitionResponse(partition.partition(), ErrorCode.NONE));
            }
            topicResponses.add(new OffsetCommitTopicResponse(topic.topic(), partitionResponses));
        }
        metrics.recordGroup(
                ApiKey.OFFSET_COMMIT,
                body.groupId(),
                ErrorCode.NONE,
                System.currentTimeMillis() - startMs);
        return ResponseContext.builder()
                .requestContext(requestContext)
                .apiKey(ApiKey.OFFSET_COMMIT)
                .apiVersion((short) 0)
                .responseHeader(new ResponseHeader(requestContext.getCorrelationId(), (short) 2, ErrorCode.NONE, 0))
                .responseBody(new OffsetCommitResponseBody(topicResponses))
                .build();
    }
}
