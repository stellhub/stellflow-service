package io.github.stellhub.stellflow.controller.quorum;

import io.github.stellhub.stellflow.config.EndpointParser;
import org.apache.ratis.protocol.RaftPeer;

/**
 * Controller quorum 节点配置。
 */
public record ControllerQuorumPeer(String id, String endpoint, String host, int port) {

    /**
     * 从文本配置解析单个 peer。
     */
    public static ControllerQuorumPeer parse(String value) {
        int separator = value.indexOf('@');
        if (separator <= 0 || separator == value.length() - 1) {
            throw new IllegalArgumentException(
                    "Expected controller quorum peer format id@grpc://host:port, but was " + value);
        }
        String id = value.substring(0, separator).trim();
        EndpointParser.ParsedEndpoint endpoint =
                EndpointParser.parse(value.substring(separator + 1).trim(), "grpc");
        return new ControllerQuorumPeer(
                id,
                endpoint.scheme() + "://" + endpoint.host() + ":" + endpoint.port(),
                endpoint.host(),
                endpoint.port());
    }

    /**
     * 转换为 Ratis peer。
     */
    public RaftPeer toRaftPeer() {
        return RaftPeer.newBuilder().setId(id).setAddress(host + ":" + port).build();
    }
}
