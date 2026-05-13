package io.github.stellhub.stellflow.server.api;

import io.github.stellhub.stellflow.coordinator.ConsumerGroupCoordinator;
import io.github.stellhub.stellflow.coordinator.OffsetStore;
import io.github.stellhub.stellflow.metadata.MetadataCache;
import io.github.stellhub.stellflow.network.protocol.ApiKey;
import io.github.stellhub.stellflow.network.protocol.EmptyResponseBody;
import io.github.stellhub.stellflow.network.protocol.ErrorCode;
import io.github.stellhub.stellflow.network.protocol.ResponseHeader;
import io.github.stellhub.stellflow.observability.metrics.StellflowMetrics;
import io.github.stellhub.stellflow.producer.ProducerStateManager;
import io.github.stellhub.stellflow.controller.replica.ControllerReplicaCoordinator;
import io.github.stellhub.stellflow.server.runtime.ReplicaManager;
import io.github.stellhub.stellflow.security.RequestGovernance;
import io.github.stellhub.stellflow.storage.log.LogManager;
import io.github.stellhub.stellflow.storage.log.LogStorageConfig;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/**
 * 统一 API 分发入口。
 */
@Slf4j
public class BrokerApis implements AutoCloseable {

    private final Map<ApiKey, ApiHandler> handlers;
    private final AutoCloseable closeableResource;
    private final LogManager logManager;
    private final ControllerReplicaCoordinator controllerReplicaCoordinator;
    private final MetadataCache metadataCache;
    private final ReplicaManager replicaManager;
    private final RequestGovernance requestGovernance;
    private final StellflowMetrics metrics;

    public BrokerApis(Map<ApiKey, ApiHandler> handlers) {
        this(handlers, () -> {}, null, null, null, null, RequestGovernance.allowAll(), StellflowMetrics.global());
    }

    public BrokerApis(Map<ApiKey, ApiHandler> handlers, AutoCloseable closeableResource) {
        this(
                handlers,
                closeableResource,
                null,
                null,
                null,
                null,
                RequestGovernance.allowAll(),
                StellflowMetrics.global());
    }

    public BrokerApis(
            Map<ApiKey, ApiHandler> handlers,
            AutoCloseable closeableResource,
            LogManager logManager,
            ControllerReplicaCoordinator controllerReplicaCoordinator) {
        this(
                handlers,
                closeableResource,
                logManager,
                controllerReplicaCoordinator,
                null,
                null,
                RequestGovernance.allowAll(),
                StellflowMetrics.global());
    }

    public BrokerApis(
            Map<ApiKey, ApiHandler> handlers,
            AutoCloseable closeableResource,
            LogManager logManager,
            ControllerReplicaCoordinator controllerReplicaCoordinator,
            MetadataCache metadataCache,
            ReplicaManager replicaManager) {
        this(
                handlers,
                closeableResource,
                logManager,
                controllerReplicaCoordinator,
                metadataCache,
                replicaManager,
                RequestGovernance.allowAll(),
                StellflowMetrics.global());
    }

    public BrokerApis(
            Map<ApiKey, ApiHandler> handlers,
            AutoCloseable closeableResource,
            LogManager logManager,
            ControllerReplicaCoordinator controllerReplicaCoordinator,
            MetadataCache metadataCache,
            ReplicaManager replicaManager,
            RequestGovernance requestGovernance) {
        this(
                handlers,
                closeableResource,
                logManager,
                controllerReplicaCoordinator,
                metadataCache,
                replicaManager,
                requestGovernance,
                StellflowMetrics.global());
    }

    public BrokerApis(
            Map<ApiKey, ApiHandler> handlers,
            AutoCloseable closeableResource,
            LogManager logManager,
            ControllerReplicaCoordinator controllerReplicaCoordinator,
            MetadataCache metadataCache,
            ReplicaManager replicaManager,
            RequestGovernance requestGovernance,
            StellflowMetrics metrics) {
        this.handlers = Map.copyOf(handlers);
        this.closeableResource = closeableResource;
        this.logManager = logManager;
        this.controllerReplicaCoordinator = controllerReplicaCoordinator;
        this.metadataCache = metadataCache;
        this.replicaManager = replicaManager;
        this.requestGovernance = requestGovernance;
        this.metrics = metrics;
    }

    /**
     * 处理请求并生成响应。
     */
    public ResponseContext handleRequest(RequestContext requestContext) {
        long startMs = System.currentTimeMillis();
        ApiHandler handler = handlers.get(requestContext.getApiKey());
        if (handler == null) {
            log.warn(
                    "No handler registered for apiKey={} apiVersion={} correlationId={}",
                    requestContext.getApiKey(),
                    requestContext.getApiVersion(),
                    requestContext.getCorrelationId());
            return recordAndReturn(
                    buildErrorResponse(requestContext, ErrorCode.FEATURE_NOT_ENABLED), startMs);
        }
        ErrorCode governanceError = requestGovernance.evaluate(requestContext);
        if (governanceError != ErrorCode.NONE) {
            return recordAndReturn(buildErrorResponse(requestContext, governanceError), startMs);
        }
        try {
            return recordAndReturn(handler.handle(requestContext), startMs);
        } catch (RuntimeException exception) {
            log.error(
                    "Request handling failed apiKey={} apiVersion={} correlationId={}",
                    requestContext.getApiKey(),
                    requestContext.getApiVersion(),
                    requestContext.getCorrelationId(),
                    exception);
            return recordAndReturn(
                    buildErrorResponse(requestContext, ErrorCode.UNKNOWN_SERVER_ERROR), startMs);
        }
    }

    /**
     * 创建默认 BrokerApis。
     */
    public static BrokerApis defaultBrokerApis() {
        return defaultBrokerApis(0, "127.0.0.1", 9092, LogStorageConfig.load().getRootDir());
    }

    /**
     * 创建带显式 broker 地址的默认 BrokerApis。
     */
    public static BrokerApis defaultBrokerApis(String advertisedHost, int advertisedPort) {
        return defaultBrokerApis(
                0, advertisedHost, advertisedPort, LogStorageConfig.load().getRootDir());
    }

    /**
     * 创建带显式 brokerId 与地址的默认 BrokerApis。
     */
    public static BrokerApis defaultBrokerApis(
            int brokerId, String advertisedHost, int advertisedPort) {
        return defaultBrokerApis(
                brokerId, advertisedHost, advertisedPort, LogStorageConfig.load().getRootDir());
    }

    /**
     * 创建带显式 broker 地址与日志目录的默认 BrokerApis。
     */
    public static BrokerApis defaultBrokerApis(
            String advertisedHost, int advertisedPort, Path logRootDir) {
        return defaultBrokerApis(0, advertisedHost, advertisedPort, logRootDir);
    }

    /**
     * 创建带显式 brokerId、地址与日志目录的默认 BrokerApis。
     */
    public static BrokerApis defaultBrokerApis(
            int brokerId, String advertisedHost, int advertisedPort, Path logRootDir) {
        StellflowMetrics metrics = StellflowMetrics.global();
        LogManager logManager = new LogManager(logRootDir, metrics);
        MetadataCache metadataCache = new MetadataCache(brokerId);
        metadataCache.registerLocalBroker(advertisedHost, advertisedPort);
        metadataCache.bootstrapFromLogManager(logManager);
        ReplicaManager replicaManager = new ReplicaManager(logManager, metadataCache, true);
        OffsetStore offsetStore = new OffsetStore(logRootDir.resolve("__consumer_offsets.snapshot"));
        ConsumerGroupCoordinator groupCoordinator = new ConsumerGroupCoordinator(offsetStore);
        ProducerStateManager producerStateManager = new ProducerStateManager();
        ControllerReplicaCoordinator controllerReplicaCoordinator =
                new ControllerReplicaCoordinator(replicaManager);
        Map<ApiKey, ApiHandler> handlers = new HashMap<>();
        handlers.put(ApiKey.API_VERSIONS, new ApiVersionsHandler());
        handlers.put(ApiKey.METADATA, new MetadataHandler(metadataCache, advertisedHost, advertisedPort));
        handlers.put(ApiKey.PRODUCE, new ProduceHandler(replicaManager, producerStateManager, metrics));
        handlers.put(ApiKey.FETCH, new FetchHandler(replicaManager, metrics));
        handlers.put(ApiKey.LIST_OFFSETS, new ListOffsetsHandler(replicaManager));
        handlers.put(ApiKey.FIND_COORDINATOR, new FindCoordinatorHandler(0, advertisedHost, advertisedPort));
        handlers.put(ApiKey.OFFSET_COMMIT, new OffsetCommitHandler(offsetStore, metrics));
        handlers.put(ApiKey.OFFSET_FETCH, new OffsetFetchHandler(offsetStore, metrics));
        handlers.put(ApiKey.HEARTBEAT, new HeartbeatHandler(groupCoordinator, metrics));
        handlers.put(ApiKey.JOIN_GROUP, new JoinGroupHandler(groupCoordinator, metrics));
        handlers.put(ApiKey.SYNC_GROUP, new SyncGroupHandler(groupCoordinator, metrics));
        handlers.put(ApiKey.INIT_PRODUCER_ID, new InitProducerIdHandler(producerStateManager));
        handlers.put(
                ApiKey.BEGIN_TRANSACTION,
                new TransactionHandler(ApiKey.BEGIN_TRANSACTION, producerStateManager));
        handlers.put(
                ApiKey.END_TRANSACTION,
                new TransactionHandler(ApiKey.END_TRANSACTION, producerStateManager));
        handlers.put(ApiKey.CREATE_TOPIC, new TopicAdminHandler(ApiKey.CREATE_TOPIC, replicaManager));
        handlers.put(ApiKey.DELETE_TOPIC, new TopicAdminHandler(ApiKey.DELETE_TOPIC, replicaManager));
        handlers.put(ApiKey.ALTER_PARTITION, new TopicAdminHandler(ApiKey.ALTER_PARTITION, replicaManager));
        handlers.put(ApiKey.DESCRIBE_CLUSTER, new ClusterStatusHandler(ApiKey.DESCRIBE_CLUSTER, metadataCache));
        handlers.put(ApiKey.HEALTH_CHECK, new ClusterStatusHandler(ApiKey.HEALTH_CHECK, metadataCache));
        handlers.put(ApiKey.DECOMMISSION_BROKER, new BrokerAdminHandler());
        return new BrokerApis(
                handlers,
                logManager,
                logManager,
                controllerReplicaCoordinator,
                metadataCache,
                replicaManager,
                RequestGovernance.allowAll(),
                metrics);
    }

    /**
     * 返回底层日志管理器。
     */
    public LogManager logManager() {
        return logManager;
    }

    /**
     * 返回控制面到副本层的协调适配器。
     */
    public ControllerReplicaCoordinator controllerReplicaCoordinator() {
        return controllerReplicaCoordinator;
    }

    /**
     * 返回 Broker 本地元数据缓存。
     */
    public MetadataCache metadataCache() {
        return metadataCache;
    }

    /**
     * 返回副本运行时管理器。
     */
    public ReplicaManager replicaManager() {
        return replicaManager;
    }

    /**
     * 返回指标聚合器。
     */
    public StellflowMetrics metrics() {
        return metrics;
    }

    @Override
    public void close() {
        try {
            closeableResource.close();
        } catch (Exception exception) {
            throw new IllegalStateException("Failed to close BrokerApis resource", exception);
        }
    }

    private ResponseContext buildErrorResponse(RequestContext requestContext, ErrorCode errorCode) {
        return ResponseContext.builder()
                .requestContext(requestContext)
                .apiKey(requestContext.getApiKey())
                .apiVersion(requestContext.getApiVersion())
                .responseHeader(
                        new ResponseHeader(
                                requestContext.getCorrelationId(), (short) 2, errorCode, 0))
                .responseBody(EmptyResponseBody.INSTANCE)
                .build();
    }

    private ResponseContext recordAndReturn(ResponseContext responseContext, long startMs) {
        ErrorCode errorCode =
                responseContext.getResponseHeader() == null
                        ? ErrorCode.UNKNOWN_SERVER_ERROR
                        : responseContext.getResponseHeader().errorCode();
        metrics.recordBrokerRequest(
                responseContext.getApiKey(), errorCode, System.currentTimeMillis() - startMs);
        return responseContext;
    }
}
