package io.github.stellhub.stellflow.controller.quorum;

import io.github.stellhub.stellflow.controller.control.ControllerMetadataStateMachine;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;

/**
 * Controller 元数据的 Ratis 状态机适配器。
 */
@Slf4j
public class ControllerRaftStateMachine extends BaseStateMachine {

    private final ControllerMetadataStateMachine metadataStateMachine;

    public ControllerRaftStateMachine(ControllerMetadataStateMachine metadataStateMachine) {
        this.metadataStateMachine = metadataStateMachine;
    }

    /**
     * 将客户端请求转换为可复制的状态机事务。
     */
    @Override
    public TransactionContext startTransaction(org.apache.ratis.protocol.RaftClientRequest request)
            throws IOException {
        return TransactionContext.newBuilder()
                .setStateMachine(this)
                .setClientRequest(request)
                .setLogData(request.getMessage().getContent())
                .build();
    }

    /**
     * 应用已提交事务到本地 controller 元数据状态机。
     */
    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
        try {
            byte[] bytes = trx.getStateMachineLogEntry().getLogData().toByteArray();
            ControllerMetadataRecord record = ControllerMetadataRecordCodec.decode(bytes);
            applyRecord(record);
            if (trx.getLogEntry() != null) {
                updateLastAppliedTermIndex(trx.getLogEntry().getTerm(), trx.getLogEntry().getIndex());
            }
            return CompletableFuture.completedFuture(Message.valueOf("OK"));
        } catch (RuntimeException exception) {
            log.error("Failed to apply controller metadata transaction", exception);
            CompletableFuture<Message> future = new CompletableFuture<>();
            future.completeExceptionally(exception);
            return future;
        }
    }

    private void applyRecord(ControllerMetadataRecord record) {
        switch (record.type()) {
            case REGISTER_BROKER ->
                    metadataStateMachine.registerBroker(
                            record.brokerId(),
                            record.advertisedEndpoint(),
                            record.advertisedHost(),
                            record.advertisedPort(),
                            record.registeredAtMs());
            case UPSERT_PARTITION ->
                    metadataStateMachine.upsertPartition(
                            ControllerMetadataRecordCodec.toPartitionMetadata(record));
            case REMOVE_PARTITION ->
                    metadataStateMachine.removePartition(record.topic(), record.partition());
        }
    }
}
