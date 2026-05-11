package io.github.stellhub.stellflow.observability.metrics;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

/**
 * Replica Fetch 指标聚合器。
 */
public class ReplicaFetchMetrics {

    private final Map<String, PartitionMetricState> partitions = new ConcurrentHashMap<>();
    private final Meter meter =
            GlobalOpenTelemetry.get().meterBuilder("io.github.stellhub.stellflow.replica").build();
    private final LongCounter requestsCounter =
            meter.counterBuilder("stellflow.replica.fetch.requests").build();
    private final LongCounter failuresCounter =
            meter.counterBuilder("stellflow.replica.fetch.failures").build();
    private final LongCounter bytesCounter =
            meter.counterBuilder("stellflow.replica.fetch.bytes").setUnit("By").build();
    private final LongCounter entriesCounter =
            meter.counterBuilder("stellflow.replica.fetch.entries").build();

    public ReplicaFetchMetrics() {
        meter.gaugeBuilder("stellflow.replica.fetch.lag")
                .ofLongs()
                .buildWithCallback(
                        measurement ->
                                recordPartitions(
                                        measurement,
                                        (observer, state) ->
                                                observer.record(state.lag.get(), state.attributes)));
        meter.gaugeBuilder("stellflow.replica.fetch.last.success.timestamp")
                .ofLongs()
                .setUnit("ms")
                .buildWithCallback(
                        measurement ->
                                recordPartitions(
                                        measurement,
                                        (observer, state) ->
                                                observer.record(
                                                        state.lastSuccessTimestampMs.get(),
                                                        state.attributes)));
    }

    /**
     * 记录一次成功抓取。
     */
    public void recordSuccess(
            String topic,
            int partition,
            int leaderBrokerId,
            long bytes,
            long entries,
            long lag,
            long fetchTimestampMs) {
        PartitionMetricState state = state(topic, partition, leaderBrokerId);
        requestsCounter.add(1, state.attributes);
        bytesCounter.add(Math.max(0, bytes), state.attributes);
        entriesCounter.add(Math.max(0, entries), state.attributes);
        state.requestsTotal.incrementAndGet();
        state.bytesTotal.addAndGet(Math.max(0, bytes));
        state.entriesTotal.addAndGet(Math.max(0, entries));
        state.lag.set(Math.max(0, lag));
        state.lastSuccessTimestampMs.set(fetchTimestampMs);
    }

    /**
     * 记录一次失败抓取。
     */
    public void recordFailure(
            String topic, int partition, int leaderBrokerId, long lag, long failureTimestampMs) {
        PartitionMetricState state = state(topic, partition, leaderBrokerId);
        failuresCounter.add(1, state.attributes);
        state.failuresTotal.incrementAndGet();
        state.lag.set(Math.max(0, lag));
        state.lastFailureTimestampMs.set(failureTimestampMs);
    }

    /**
     * 生成 Prometheus 文本格式输出。
     */
    public String renderPrometheus() {
        StringBuilder builder = new StringBuilder(2048);
        appendHelp(builder, "stellflow_replica_fetch_requests_total", "Replica fetch request count");
        appendHelp(builder, "stellflow_replica_fetch_failures_total", "Replica fetch failure count");
        appendHelp(builder, "stellflow_replica_fetch_bytes_total", "Replica fetch bytes count");
        appendHelp(builder, "stellflow_replica_fetch_entries_total", "Replica fetch entries count");
        appendHelp(builder, "stellflow_replica_fetch_lag", "Replica fetch lag");
        appendHelp(
                builder,
                "stellflow_replica_fetch_last_success_timestamp_ms",
                "Last successful replica fetch timestamp in milliseconds");
        appendHelp(
                builder,
                "stellflow_replica_fetch_last_failure_timestamp_ms",
                "Last failed replica fetch timestamp in milliseconds");
        for (PartitionMetricState state : partitions.values()) {
            String labels = state.prometheusLabels();
            appendSample(
                    builder,
                    "stellflow_replica_fetch_requests_total",
                    labels,
                    state.requestsTotal.get());
            appendSample(
                    builder,
                    "stellflow_replica_fetch_failures_total",
                    labels,
                    state.failuresTotal.get());
            appendSample(
                    builder, "stellflow_replica_fetch_bytes_total", labels, state.bytesTotal.get());
            appendSample(
                    builder,
                    "stellflow_replica_fetch_entries_total",
                    labels,
                    state.entriesTotal.get());
            appendSample(builder, "stellflow_replica_fetch_lag", labels, state.lag.get());
            appendSample(
                    builder,
                    "stellflow_replica_fetch_last_success_timestamp_ms",
                    labels,
                    state.lastSuccessTimestampMs.get());
            appendSample(
                    builder,
                    "stellflow_replica_fetch_last_failure_timestamp_ms",
                    labels,
                    state.lastFailureTimestampMs.get());
        }
        return builder.toString();
    }

    private void recordPartitions(
            ObservableLongMeasurement measurement,
            BiConsumer<ObservableLongMeasurement, PartitionMetricState> consumer) {
        for (PartitionMetricState state : partitions.values()) {
            consumer.accept(measurement, state);
        }
    }

    private PartitionMetricState state(String topic, int partition, int leaderBrokerId) {
        String key = topic + ":" + partition + ":" + leaderBrokerId;
        return partitions.computeIfAbsent(
                key, ignored -> new PartitionMetricState(topic, partition, leaderBrokerId));
    }

    private void appendHelp(StringBuilder builder, String name, String help) {
        builder.append("# HELP ").append(name).append(' ').append(help).append('\n');
        builder.append("# TYPE ").append(name).append(name.endsWith("_total") ? " counter" : " gauge")
                .append('\n');
    }

    private void appendSample(StringBuilder builder, String name, String labels, long value) {
        builder.append(name).append(labels).append(' ').append(value).append('\n');
    }

    /**
     * 单分区副本抓取指标状态。
     */
    private static final class PartitionMetricState {

        private final Attributes attributes;
        private final String prometheusLabels;
        private final AtomicLong requestsTotal = new AtomicLong();
        private final AtomicLong failuresTotal = new AtomicLong();
        private final AtomicLong bytesTotal = new AtomicLong();
        private final AtomicLong entriesTotal = new AtomicLong();
        private final AtomicLong lag = new AtomicLong();
        private final AtomicLong lastSuccessTimestampMs = new AtomicLong();
        private final AtomicLong lastFailureTimestampMs = new AtomicLong();

        private PartitionMetricState(String topic, int partition, int leaderBrokerId) {
            this.attributes =
                    Attributes.builder()
                            .put("topic", topic)
                            .put("partition", partition)
                            .put("leader.broker.id", leaderBrokerId)
                            .build();
            this.prometheusLabels =
                    "{topic=\""
                            + escape(topic)
                            + "\",partition=\""
                            + partition
                            + "\",leader_broker_id=\""
                            + leaderBrokerId
                            + "\"}";
        }

        private String prometheusLabels() {
            return prometheusLabels;
        }

        private static String escape(String value) {
            return value.replace("\\", "\\\\").replace("\"", "\\\"");
        }
    }
}
