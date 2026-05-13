package io.github.stellhub.stellflow.observability.metrics;

import io.github.stellhub.stellflow.network.protocol.ApiKey;
import io.github.stellhub.stellflow.network.protocol.ErrorCode;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Stellflow 核心 Prometheus 指标聚合器。
 */
public class StellflowMetrics {

    private static final StellflowMetrics GLOBAL = new StellflowMetrics();

    private final Map<MetricKey, CounterState> brokerRequests = new ConcurrentHashMap<>();
    private final Map<MetricKey, CounterState> produces = new ConcurrentHashMap<>();
    private final Map<MetricKey, CounterState> fetches = new ConcurrentHashMap<>();
    private final Map<MetricKey, CounterState> groups = new ConcurrentHashMap<>();
    private final Map<MetricKey, CounterState> controllers = new ConcurrentHashMap<>();
    private final Map<MetricKey, CounterState> storageOperations = new ConcurrentHashMap<>();
    private final Map<MetricKey, GaugeState> storagePartitions = new ConcurrentHashMap<>();

    /**
     * 返回进程级默认指标实例。
     */
    public static StellflowMetrics global() {
        return GLOBAL;
    }

    /**
     * 记录 Broker API 请求。
     */
    public void recordBrokerRequest(ApiKey apiKey, ErrorCode errorCode, long durationMs) {
        increment(
                brokerRequests,
                MetricKey.of("api_key", apiKey.name(), "error_code", errorCode.name()),
                0,
                durationMs);
    }

    /**
     * 记录 Produce 分区请求。
     */
    public void recordProduce(
            String topic, int partition, ErrorCode errorCode, long bytes, long durationMs) {
        increment(
                produces,
                MetricKey.of(
                        "topic",
                        topic,
                        "partition",
                        Integer.toString(partition),
                        "error_code",
                        errorCode.name()),
                bytes,
                durationMs);
    }

    /**
     * 记录 Fetch 分区请求。
     */
    public void recordFetch(
            String fetchType,
            String topic,
            int partition,
            ErrorCode errorCode,
            long bytes,
            long durationMs) {
        increment(
                fetches,
                MetricKey.of(
                        "fetch_type",
                        fetchType,
                        "topic",
                        topic,
                        "partition",
                        Integer.toString(partition),
                        "error_code",
                        errorCode.name()),
                bytes,
                durationMs);
    }

    /**
     * 记录 Consumer Group 请求。
     */
    public void recordGroup(ApiKey apiKey, String groupId, ErrorCode errorCode, long durationMs) {
        increment(
                groups,
                MetricKey.of(
                        "api_key",
                        apiKey.name(),
                        "group_id",
                        safeLabelValue(groupId),
                        "error_code",
                        errorCode.name()),
                0,
                durationMs);
    }

    /**
     * 记录 Controller 操作。
     */
    public void recordController(String operation, String outcome, long durationMs) {
        increment(
                controllers,
                MetricKey.of("operation", operation, "outcome", outcome),
                0,
                durationMs);
    }

    /**
     * 记录 Storage 操作。
     */
    public void recordStorageOperation(
            String operation, String topic, int partition, String outcome, long bytes, long durationMs) {
        increment(
                storageOperations,
                MetricKey.of(
                        "operation",
                        operation,
                        "topic",
                        topic,
                        "partition",
                        Integer.toString(partition),
                        "outcome",
                        outcome),
                bytes,
                durationMs);
    }

    /**
     * 更新 Storage 分区状态。
     */
    public void updateStoragePartition(
            String topic, int partition, long logEndOffset, long highWatermark) {
        storagePartitions.put(
                MetricKey.of("topic", topic, "partition", Integer.toString(partition)),
                new GaugeState(Math.max(0, logEndOffset), Math.max(0, highWatermark)));
    }

    /**
     * 输出 Prometheus 文本格式。
     */
    public String renderPrometheus() {
        StringBuilder builder = new StringBuilder(4096);
        appendCounterFamily(
                builder,
                "stellflow_broker",
                "Broker API request count",
                "Broker API request latency in milliseconds",
                brokerRequests);
        appendCounterFamily(
                builder,
                "stellflow_produce",
                "Produce partition request count",
                "Produce partition latency in milliseconds",
                produces);
        appendBytesFamily(builder, "stellflow_produce_bytes_total", "Produce bytes count", produces);
        appendCounterFamily(
                builder,
                "stellflow_fetch",
                "Fetch partition request count",
                "Fetch partition latency in milliseconds",
                fetches);
        appendBytesFamily(builder, "stellflow_fetch_bytes_total", "Fetch bytes count", fetches);
        appendCounterFamily(
                builder,
                "stellflow_group",
                "Consumer group request count",
                "Consumer group request latency in milliseconds",
                groups);
        appendCounterFamily(
                builder,
                "stellflow_controller",
                "Controller operation count",
                "Controller operation latency in milliseconds",
                controllers);
        appendCounterFamily(
                builder,
                "stellflow_storage_operations",
                "Storage operation count",
                "Storage operation latency in milliseconds",
                storageOperations);
        appendBytesFamily(
                builder, "stellflow_storage_operation_bytes_total", "Storage operation bytes count", storageOperations);
        appendStoragePartitionGauges(builder);
        return builder.toString();
    }

    private void increment(
            Map<MetricKey, CounterState> target, MetricKey key, long bytes, long durationMs) {
        CounterState state = target.computeIfAbsent(key, ignored -> new CounterState());
        state.count.incrementAndGet();
        state.bytes.addAndGet(Math.max(0, bytes));
        state.durationMs.addAndGet(Math.max(0, durationMs));
    }

    private void appendCounterFamily(
            StringBuilder builder,
            String baseName,
            String countHelp,
            String latencyHelp,
            Map<MetricKey, CounterState> values) {
        appendHelp(builder, baseName + "_requests_total", countHelp, "counter");
        for (Map.Entry<MetricKey, CounterState> entry : sorted(values).entrySet()) {
            appendSample(
                    builder,
                    baseName + "_requests_total",
                    entry.getKey().labels(),
                    entry.getValue().count.get());
        }
        appendHelp(builder, baseName + "_latency_ms_total", latencyHelp, "counter");
        for (Map.Entry<MetricKey, CounterState> entry : sorted(values).entrySet()) {
            appendSample(
                    builder,
                    baseName + "_latency_ms_total",
                    entry.getKey().labels(),
                    entry.getValue().durationMs.get());
        }
    }

    private void appendBytesFamily(
            StringBuilder builder, String metricName, String help, Map<MetricKey, CounterState> values) {
        appendHelp(builder, metricName, help, "counter");
        for (Map.Entry<MetricKey, CounterState> entry : sorted(values).entrySet()) {
            appendSample(builder, metricName, entry.getKey().labels(), entry.getValue().bytes.get());
        }
    }

    private void appendStoragePartitionGauges(StringBuilder builder) {
        appendHelp(builder, "stellflow_storage_log_end_offset", "Storage log end offset", "gauge");
        for (Map.Entry<MetricKey, GaugeState> entry : sorted(storagePartitions).entrySet()) {
            appendSample(
                    builder,
                    "stellflow_storage_log_end_offset",
                    entry.getKey().labels(),
                    entry.getValue().logEndOffset);
        }
        appendHelp(builder, "stellflow_storage_high_watermark", "Storage high watermark", "gauge");
        for (Map.Entry<MetricKey, GaugeState> entry : sorted(storagePartitions).entrySet()) {
            appendSample(
                    builder,
                    "stellflow_storage_high_watermark",
                    entry.getKey().labels(),
                    entry.getValue().highWatermark);
        }
    }

    private <T> Map<MetricKey, T> sorted(Map<MetricKey, T> values) {
        return new TreeMap<>(values);
    }

    private void appendHelp(StringBuilder builder, String name, String help, String type) {
        builder.append("# HELP ").append(name).append(' ').append(help).append('\n');
        builder.append("# TYPE ").append(name).append(' ').append(type).append('\n');
    }

    private void appendSample(StringBuilder builder, String name, String labels, long value) {
        builder.append(name).append(labels).append(' ').append(value).append('\n');
    }

    private static String safeLabelValue(String value) {
        return value == null || value.isBlank() ? "unknown" : value;
    }

    private static final class CounterState {
        private final AtomicLong count = new AtomicLong();
        private final AtomicLong bytes = new AtomicLong();
        private final AtomicLong durationMs = new AtomicLong();
    }

    private record GaugeState(long logEndOffset, long highWatermark) {}

    private record MetricKey(Map<String, String> labelValues) implements Comparable<MetricKey> {

        private static MetricKey of(String... keyValues) {
            if (keyValues.length % 2 != 0) {
                throw new IllegalArgumentException("Metric labels must be key/value pairs");
            }
            Map<String, String> labels = new TreeMap<>();
            for (int index = 0; index < keyValues.length; index += 2) {
                labels.put(keyValues[index], safeLabelValue(keyValues[index + 1]));
            }
            return new MetricKey(Map.copyOf(labels));
        }

        private String labels() {
            StringBuilder builder = new StringBuilder("{");
            boolean first = true;
            for (Map.Entry<String, String> entry : new TreeMap<>(labelValues).entrySet()) {
                if (!first) {
                    builder.append(',');
                }
                first = false;
                builder.append(entry.getKey())
                        .append("=\"")
                        .append(escape(entry.getValue()))
                        .append('"');
            }
            return builder.append('}').toString();
        }

        @Override
        public int compareTo(MetricKey other) {
            return labels().compareTo(other.labels());
        }

        private static String escape(String value) {
            return value.replace("\\", "\\\\").replace("\"", "\\\"");
        }
    }
}
