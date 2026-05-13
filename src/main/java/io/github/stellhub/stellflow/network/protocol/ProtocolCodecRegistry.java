package io.github.stellhub.stellflow.network.protocol;

import io.netty.buffer.ByteBuf;
import java.util.HashMap;
import java.util.Map;

/**
 * 协议编解码注册表。
 */
public class ProtocolCodecRegistry {

    private final Map<ProtocolVersionKey, RequestBodyCodec<? extends RequestBody>> requestCodecs =
            new HashMap<>();
    private final Map<ProtocolVersionKey, ResponseBodyCodec<? extends ResponseBody>> responseCodecs =
            new HashMap<>();

    /**
     * 注册请求体编解码器。
     */
    public void registerRequestCodec(RequestBodyCodec<? extends RequestBody> codec) {
        requestCodecs.put(new ProtocolVersionKey(codec.apiKey(), codec.apiVersion()), codec);
    }

    /**
     * 注册响应体编解码器。
     */
    public void registerResponseCodec(ResponseBodyCodec<? extends ResponseBody> codec) {
        responseCodecs.put(new ProtocolVersionKey(codec.apiKey(), codec.apiVersion()), codec);
    }

    /**
     * 解码请求体。
     */
    public RequestBody decodeRequestBody(RequestHeader header, ByteBuf buffer) {
        RequestBodyCodec<? extends RequestBody> codec =
                requestCodecs.get(new ProtocolVersionKey(header.apiKey(), header.apiVersion()));
        if (codec == null) {
            byte[] payload = new byte[buffer.readableBytes()];
            buffer.readBytes(payload);
            return new OpaqueRequestBody(header.apiKey(), header.apiVersion(), payload);
        }
        return codec.decode(buffer);
    }

    /**
     * 编码响应体。
     */
    @SuppressWarnings("unchecked")
    public void encodeResponseBody(ApiKey apiKey, short apiVersion, ResponseBody body, ByteBuf buffer) {
        if (body instanceof EmptyResponseBody) {
            return;
        }
        ResponseBodyCodec<ResponseBody> codec =
                (ResponseBodyCodec<ResponseBody>)
                        responseCodecs.get(new ProtocolVersionKey(apiKey, apiVersion));
        if (codec == null) {
            throw new ProtocolEncodingException(
                    "No response codec for apiKey=%s, apiVersion=%s".formatted(apiKey, apiVersion));
        }
        codec.encode(body, buffer);
    }

    /**
     * 创建默认注册表。
     */
    public static ProtocolCodecRegistry defaultRegistry() {
        ProtocolCodecRegistry registry = new ProtocolCodecRegistry();
        registry.registerRequestCodec(new ApiVersionsRequestBodyCodec());
        registry.registerRequestCodec(new MetadataRequestBodyCodec());
        registry.registerRequestCodec(new ProduceRequestBodyCodec());
        registry.registerRequestCodec(new FetchRequestBodyCodec());
        registry.registerRequestCodec(new ListOffsetsRequestBodyCodec());
        registry.registerRequestCodec(new FindCoordinatorRequestBodyCodec());
        registry.registerRequestCodec(new OffsetCommitRequestBodyCodec());
        registry.registerRequestCodec(new OffsetFetchRequestBodyCodec());
        registry.registerRequestCodec(new GroupRequestBodyCodec(ApiKey.HEARTBEAT));
        registry.registerRequestCodec(new JoinGroupRequestBodyCodec());
        registry.registerRequestCodec(new GroupRequestBodyCodec(ApiKey.SYNC_GROUP));
        registry.registerRequestCodec(new InitProducerIdRequestBodyCodec());
        registry.registerRequestCodec(new TransactionRequestBodyCodec(ApiKey.BEGIN_TRANSACTION));
        registry.registerRequestCodec(new TransactionRequestBodyCodec(ApiKey.END_TRANSACTION));
        registry.registerRequestCodec(new TopicAdminRequestBodyCodec(ApiKey.CREATE_TOPIC));
        registry.registerRequestCodec(new TopicAdminRequestBodyCodec(ApiKey.DELETE_TOPIC));
        registry.registerRequestCodec(new TopicAdminRequestBodyCodec(ApiKey.ALTER_PARTITION));
        registry.registerRequestCodec(new ApiVersionsRequestBodyCodec() {
            @Override
            public ApiKey apiKey() {
                return ApiKey.DESCRIBE_CLUSTER;
            }
        });
        registry.registerRequestCodec(new ApiVersionsRequestBodyCodec() {
            @Override
            public ApiKey apiKey() {
                return ApiKey.HEALTH_CHECK;
            }
        });
        registry.registerRequestCodec(new BrokerAdminRequestBodyCodec());
        registry.registerResponseCodec(new ApiVersionsResponseBodyCodec());
        registry.registerResponseCodec(new MetadataResponseBodyCodec());
        registry.registerResponseCodec(new ProduceResponseBodyCodec());
        registry.registerResponseCodec(new FetchResponseBodyCodec());
        registry.registerResponseCodec(new ListOffsetsResponseBodyCodec());
        registry.registerResponseCodec(new FindCoordinatorResponseBodyCodec());
        registry.registerResponseCodec(new OffsetCommitResponseBodyCodec());
        registry.registerResponseCodec(new OffsetFetchResponseBodyCodec());
        registry.registerResponseCodec(new GroupResponseBodyCodec(ApiKey.HEARTBEAT));
        registry.registerResponseCodec(new JoinGroupResponseBodyCodec());
        registry.registerResponseCodec(new GroupResponseBodyCodec(ApiKey.SYNC_GROUP));
        registry.registerResponseCodec(new InitProducerIdResponseBodyCodec());
        registry.registerResponseCodec(new TransactionResponseBodyCodec(ApiKey.BEGIN_TRANSACTION));
        registry.registerResponseCodec(new TransactionResponseBodyCodec(ApiKey.END_TRANSACTION));
        registry.registerResponseCodec(new TopicAdminResponseBodyCodec(ApiKey.CREATE_TOPIC));
        registry.registerResponseCodec(new TopicAdminResponseBodyCodec(ApiKey.DELETE_TOPIC));
        registry.registerResponseCodec(new TopicAdminResponseBodyCodec(ApiKey.ALTER_PARTITION));
        registry.registerResponseCodec(new ClusterStatusResponseBodyCodec(ApiKey.DESCRIBE_CLUSTER));
        registry.registerResponseCodec(new ClusterStatusResponseBodyCodec(ApiKey.HEALTH_CHECK));
        registry.registerResponseCodec(new BrokerAdminResponseBodyCodec());
        return registry;
    }

    private record ProtocolVersionKey(ApiKey apiKey, short apiVersion) {}
}
