package io.github.stellhub.stellflow.observability.metrics;

import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.stellhub.stellflow.network.protocol.ApiKey;
import io.github.stellhub.stellflow.network.protocol.ErrorCode;
import org.junit.jupiter.api.Test;

/**
 * StellflowMetrics 测试。
 */
class StellflowMetricsTest {

    @Test
    void shouldRenderPrometheusMetricsForCorePlanes() {
        StellflowMetrics metrics = new StellflowMetrics();

        metrics.recordBrokerRequest(ApiKey.PRODUCE, ErrorCode.NONE, 3);
        metrics.recordProduce("orders", 0, ErrorCode.NONE, 12, 4);
        metrics.recordFetch("consumer", "orders", 0, ErrorCode.NONE, 12, 5);
        metrics.recordGroup(ApiKey.JOIN_GROUP, "group-a", ErrorCode.NONE, 6);
        metrics.recordController("metadata_create_topic", "success", 7);
        metrics.recordStorageOperation("append", "orders", 0, "success", 12, 8);
        metrics.updateStoragePartition("orders", 0, 2, 2);

        String rendered = metrics.renderPrometheus();

        assertTrue(rendered.contains("stellflow_broker_requests_total"));
        assertTrue(rendered.contains("stellflow_produce_requests_total"));
        assertTrue(rendered.contains("stellflow_fetch_requests_total"));
        assertTrue(rendered.contains("stellflow_group_requests_total"));
        assertTrue(rendered.contains("stellflow_controller_requests_total"));
        assertTrue(rendered.contains("stellflow_storage_operations_requests_total"));
        assertTrue(rendered.contains("stellflow_storage_log_end_offset"));
        assertTrue(rendered.contains("topic=\"orders\""));
    }
}
