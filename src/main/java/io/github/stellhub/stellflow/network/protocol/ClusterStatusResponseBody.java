package io.github.stellhub.stellflow.network.protocol;

/**
 * 集群状态响应体。
 */
public record ClusterStatusResponseBody(
        ErrorCode errorCode, String clusterId, int brokerCount, int topicCount, int partitionCount)
        implements ResponseBody {}
