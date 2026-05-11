package io.github.stellhub.stellflow.server.api;

import io.github.stellhub.stellflow.network.protocol.ApiKey;
import io.github.stellhub.stellflow.network.protocol.EmptyResponseBody;
import io.github.stellhub.stellflow.network.protocol.ErrorCode;
import io.github.stellhub.stellflow.network.protocol.ResponseHeader;
import io.github.stellhub.stellflow.controller.replica.ControllerReplicaCoordinator;
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

    public BrokerApis(Map<ApiKey, ApiHandler> handlers) {
        this(handlers, () -> {}, null, null);
    }

    public BrokerApis(Map<ApiKey, ApiHandler> handlers, AutoCloseable closeableResource) {
        this(handlers, closeableResource, null, null);
    }

    public BrokerApis(
            Map<ApiKey, ApiHandler> handlers,
            AutoCloseable closeableResource,
            LogManager logManager,
            ControllerReplicaCoordinator controllerReplicaCoordinator) {
        this.handlers = Map.copyOf(handlers);
        this.closeableResource = closeableResource;
        this.logManager = logManager;
        this.controllerReplicaCoordinator = controllerReplicaCoordinator;
    }

    /**
     * 处理请求并生成响应。
     */
    public ResponseContext handleRequest(RequestContext requestContext) {
        ApiHandler handler = handlers.get(requestContext.getApiKey());
        if (handler == null) {
            log.warn(
                    "No handler registered for apiKey={} apiVersion={} correlationId={}",
                    requestContext.getApiKey(),
                    requestContext.getApiVersion(),
                    requestContext.getCorrelationId());
            return buildErrorResponse(requestContext, ErrorCode.FEATURE_NOT_ENABLED);
        }
        try {
            return handler.handle(requestContext);
        } catch (RuntimeException exception) {
            log.error(
                    "Request handling failed apiKey={} apiVersion={} correlationId={}",
                    requestContext.getApiKey(),
                    requestContext.getApiVersion(),
                    requestContext.getCorrelationId(),
                    exception);
            return buildErrorResponse(requestContext, ErrorCode.UNKNOWN_SERVER_ERROR);
        }
    }

    /**
     * 创建默认 BrokerApis。
     */
    public static BrokerApis defaultBrokerApis() {
        return defaultBrokerApis("127.0.0.1", 9092, LogStorageConfig.load().getRootDir());
    }

    /**
     * 创建带显式 broker 地址的默认 BrokerApis。
     */
    public static BrokerApis defaultBrokerApis(String advertisedHost, int advertisedPort) {
        return defaultBrokerApis(
                advertisedHost, advertisedPort, LogStorageConfig.load().getRootDir());
    }

    /**
     * 创建带显式 broker 地址与日志目录的默认 BrokerApis。
     */
    public static BrokerApis defaultBrokerApis(
            String advertisedHost, int advertisedPort, Path logRootDir) {
        LogManager logManager = new LogManager(logRootDir);
        ControllerReplicaCoordinator controllerReplicaCoordinator =
                new ControllerReplicaCoordinator(logManager);
        Map<ApiKey, ApiHandler> handlers = new HashMap<>();
        handlers.put(ApiKey.API_VERSIONS, new ApiVersionsHandler());
        handlers.put(ApiKey.METADATA, new MetadataHandler(logManager, advertisedHost, advertisedPort));
        handlers.put(ApiKey.PRODUCE, new ProduceHandler(logManager));
        handlers.put(ApiKey.FETCH, new FetchHandler(logManager));
        handlers.put(ApiKey.LIST_OFFSETS, new ListOffsetsHandler(logManager));
        return new BrokerApis(handlers, logManager, logManager, controllerReplicaCoordinator);
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
}
