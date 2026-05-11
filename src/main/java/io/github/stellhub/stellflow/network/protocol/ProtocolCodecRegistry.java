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
        registry.registerResponseCodec(new ApiVersionsResponseBodyCodec());
        registry.registerResponseCodec(new MetadataResponseBodyCodec());
        registry.registerResponseCodec(new ProduceResponseBodyCodec());
        registry.registerResponseCodec(new FetchResponseBodyCodec());
        registry.registerResponseCodec(new ListOffsetsResponseBodyCodec());
        return registry;
    }

    private record ProtocolVersionKey(ApiKey apiKey, short apiVersion) {}
}
