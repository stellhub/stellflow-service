package io.github.stellhub.stellflow.controller.quorum;

import io.github.stellhub.stellflow.config.EndpointParser;
import io.github.stellhub.stellflow.controller.control.ControllerMetadataCommandService;
import io.github.stellhub.stellflow.controller.control.ControllerMetadataStateMachine;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.UUID;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.TimeDuration;

/**
 * Controller quorum 管理器。
 */
@Slf4j
public class ControllerQuorumManager implements ControllerMetadataCommandService {

    private final ControllerQuorumConfig config;
    private final ControllerMetadataStateMachine metadataStateMachine;
    private final ControllerRaftStateMachine raftStateMachine;
    @Getter private final RaftGroup raftGroup;
    @Getter private final RaftPeer selfPeer;
    private final RaftProperties raftProperties;
    private final Parameters parameters;
    private RaftServer raftServer;
    private RaftClient raftClient;

    public ControllerQuorumManager(
            ControllerQuorumConfig config, ControllerMetadataStateMachine metadataStateMachine) {
        this.config = config;
        this.metadataStateMachine = metadataStateMachine;
        this.raftStateMachine = new ControllerRaftStateMachine(metadataStateMachine);
        this.raftGroup = buildGroup(config);
        this.selfPeer =
                raftGroup.getPeer(RaftPeerId.valueOf(config.getSelfId()));
        if (selfPeer == null) {
            throw new IllegalArgumentException(
                    "Controller quorum peers do not contain selfId=" + config.getSelfId());
        }
        this.raftProperties = buildRaftProperties(config);
        this.parameters = new Parameters();
    }

    /**
     * 启动 controller quorum。
     */
    public synchronized void start() {
        if (!config.isEnabled() || raftServer != null) {
            return;
        }
        try {
            Files.createDirectories(config.getStorageDir());
            raftServer =
                    RaftServer.newBuilder()
                            .setServerId(selfPeer.getId())
                            .setGroup(raftGroup)
                            .setStateMachine(raftStateMachine)
                            .setProperties(raftProperties)
                            .setParameters(parameters)
                            .build();
            raftServer.start();
            raftClient =
                    RaftClient.newBuilder()
                            .setRaftGroup(raftGroup)
                            .setProperties(raftProperties)
                            .setParameters(parameters)
                            .build();
            log.info(
                    "Controller quorum started selfId={} endpoint={} peers={}",
                    config.getSelfId(),
                    config.getEndpoint(),
                    config.getPeers());
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to start controller quorum", exception);
        }
    }

    /**
     * 提交单条元数据记录。
     */
    @Override
    public void submit(ControllerMetadataRecord record) {
        submitRecordInternal(record);
    }

    /**
     * 关闭 controller quorum。
     */
    @Override
    public synchronized void close() {
        if (raftClient != null) {
            try {
                raftClient.close();
            } catch (IOException exception) {
                log.warn("Failed to close controller quorum client cleanly", exception);
            } finally {
                raftClient = null;
            }
        }
        if (raftServer != null) {
            try {
                raftServer.close();
            } catch (IOException exception) {
                log.warn("Failed to close controller quorum server cleanly", exception);
            } finally {
                raftServer = null;
            }
        }
    }

    private void submitRecordInternal(ControllerMetadataRecord record) {
        if (!config.isEnabled()) {
            throw new IllegalStateException("Controller quorum is not enabled");
        }
        if (raftClient == null) {
            throw new IllegalStateException("Controller quorum has not been started");
        }
        long deadlineMs = System.currentTimeMillis() + config.getRequestTimeoutMs();
        byte[] bytes = ControllerMetadataRecordCodec.encode(record);
        RuntimeException lastFailure = null;
        while (System.currentTimeMillis() <= deadlineMs) {
            try {
                RaftClientReply reply = raftClient.io().send(Message.valueOf(ByteString.copyFrom(bytes)));
                if (reply.isSuccess()) {
                    waitUntilLocallyApplied(record, deadlineMs);
                    return;
                }
                lastFailure =
                        new IllegalStateException(
                                "Controller quorum rejected metadata record: "
                                        + reply.getException());
            } catch (IOException exception) {
                lastFailure =
                        new IllegalStateException(
                                "Failed to submit controller metadata record", exception);
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException interruptedException) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(
                        "Interrupted while submitting controller metadata record", interruptedException);
            }
        }
        throw lastFailure == null
                ? new IllegalStateException("Failed to submit controller metadata record")
                : lastFailure;
    }

    private void waitUntilLocallyApplied(ControllerMetadataRecord record, long deadlineMs) {
        while (System.currentTimeMillis() <= deadlineMs) {
            if (isLocallyApplied(record)) {
                return;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException interruptedException) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(
                        "Interrupted while waiting for controller metadata apply",
                        interruptedException);
            }
        }
        throw new IllegalStateException(
                "Timed out waiting for local controller metadata apply: " + record.type());
    }

    private boolean isLocallyApplied(ControllerMetadataRecord record) {
        return switch (record.type()) {
            case REGISTER_BROKER ->
                    metadataStateMachine.broker(record.brokerId()).isPresent();
            case FENCE_BROKER ->
                    metadataStateMachine
                            .broker(record.brokerId())
                            .map(broker -> broker.fenced())
                            .orElse(false);
            case UNFENCE_BROKER ->
                    metadataStateMachine
                            .broker(record.brokerId())
                            .map(broker -> !broker.fenced())
                            .orElse(false);
            case CREATE_TOPIC ->
                    metadataStateMachine
                            .topic(record.topic())
                            .map(topic -> topic.partitionCount() == record.partitionCount())
                            .orElse(false);
            case DELETE_TOPIC -> metadataStateMachine.topic(record.topic()).isEmpty();
            case EXPAND_TOPIC_PARTITIONS ->
                    metadataStateMachine
                            .topic(record.topic())
                            .map(topic -> topic.partitionCount() == record.partitionCount())
                            .orElse(false);
            case UPDATE_PARTITION_TOPOLOGY ->
                    metadataStateMachine
                            .partition(record.topic(), record.partition())
                            .map(partition -> partition.replicaNodes().equals(record.replicaNodes()))
                            .orElse(false);
            case UPDATE_PARTITION_LEADER_ISR ->
                    metadataStateMachine
                            .partition(record.topic(), record.partition())
                            .map(
                                    partition ->
                                            partition.leaderId() == record.leaderId()
                                                    && partition.leaderEpoch() == record.leaderEpoch()
                                                    && partition.isrNodes().equals(record.isrNodes()))
                            .orElse(false);
            case SHRINK_PARTITION_ISR ->
                    metadataStateMachine
                            .partition(record.topic(), record.partition())
                            .map(partition -> !partition.isrNodes().contains(record.brokerId()))
                            .orElse(false);
            case EXPAND_PARTITION_ISR ->
                    metadataStateMachine
                            .partition(record.topic(), record.partition())
                            .map(partition -> partition.isrNodes().contains(record.brokerId()))
                            .orElse(false);
            case REMOVE_PARTITION ->
                    metadataStateMachine.partition(record.topic(), record.partition()).isEmpty();
        };
    }

    private static RaftGroup buildGroup(ControllerQuorumConfig config) {
        List<RaftPeer> peers =
                config.parsedPeers().stream().map(ControllerQuorumPeer::toRaftPeer).toList();
        return RaftGroup.valueOf(RaftGroupId.valueOf(UUID.fromString(config.getGroupId())), peers);
    }

    private static RaftProperties buildRaftProperties(ControllerQuorumConfig config) {
        RaftProperties properties = new RaftProperties();
        EndpointParser.ParsedEndpoint endpoint = config.parsedSelfEndpoint();
        RaftConfigKeys.Rpc.setType(properties, SupportedRpcType.GRPC);
        GrpcConfigKeys.Server.setHost(properties, endpoint.host());
        GrpcConfigKeys.Server.setPort(properties, endpoint.port());
        RaftServerConfigKeys.setStorageDir(properties, List.of(config.getStorageDir().toFile()));
        RaftServerConfigKeys.Rpc.setRequestTimeout(
                properties, TimeDuration.valueOf(config.getRequestTimeoutMs(), java.util.concurrent.TimeUnit.MILLISECONDS));
        return properties;
    }
}
