package io.github.stellhub.stellflow.controller.control;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * ControllerMetadataStateMachine 测试。
 */
class ControllerMetadataStateMachineTest {

    /**
     * 验证批量分区元数据会驱动 assignment 与 partition control 快照。
     */
    @Test
    void shouldDriveBatchPartitionControlFromMetadataState() {
        ControllerAssignmentRegistry assignmentRegistry = new ControllerAssignmentRegistry();
        ControllerPartitionControlRegistry partitionControlRegistry =
                new ControllerPartitionControlRegistry();
        PartitionControlResultRegistry resultRegistry = new PartitionControlResultRegistry();
        DirectControllerMetadataCommandService metadataCommandService;
        ControllerMetadataStateMachine stateMachine =
                new ControllerMetadataStateMachine(
                        assignmentRegistry, partitionControlRegistry, resultRegistry);
        metadataCommandService = new DirectControllerMetadataCommandService(stateMachine);
        ControllerMetadataDecisionService decisionService =
                new ControllerMetadataDecisionService(metadataCommandService, stateMachine);

        decisionService.registerBroker(1, "stellflow://127.0.0.1:9092", "127.0.0.1", 9092, 1L);
        decisionService.registerBroker(2, "stellflow://127.0.0.1:9093", "127.0.0.1", 9093, 2L);
        decisionService.registerBroker(3, "stellflow://127.0.0.1:9094", "127.0.0.1", 9094, 3L);

        decisionService.createTopic("orders", 1, 2);
        decisionService.createTopic("payments", 1, 2);

        assertEquals(1, partitionControlRegistry.commands(1).size());
        assertEquals(2, partitionControlRegistry.commands(2).size());
        assertEquals(1, partitionControlRegistry.commands(3).size());
        assertEquals(1, assignmentRegistry.assignments(2).size());
        assertEquals("orders", assignmentRegistry.assignments(2).get(0).getTopic());
        assertEquals(1, assignmentRegistry.assignments(3).size());
        assertEquals("payments", assignmentRegistry.assignments(3).get(0).getTopic());
        assertEquals(2, stateMachine.topics().size());
        assertEquals(2, stateMachine.partition("payments", 0).orElseThrow().leaderId());

        decisionService.shrinkIsr("orders", 0, 2);
        assertEquals(List.of(1), stateMachine.partition("orders", 0).orElseThrow().isrNodes());

        decisionService.expandIsr("orders", 0, 2);
        assertEquals(List.of(1, 2), stateMachine.partition("orders", 0).orElseThrow().isrNodes());

        decisionService.fenceBroker(1);
        assertEquals(2, stateMachine.partition("orders", 0).orElseThrow().leaderId());
        assertEquals(true, stateMachine.broker(1).orElseThrow().fenced());

        decisionService.unfenceBroker(1);
        assertEquals(false, stateMachine.broker(1).orElseThrow().fenced());

        decisionService.expandTopicPartitions("orders", 2, 2);
        assertEquals(2, stateMachine.topic("orders").orElseThrow().partitionCount());

        decisionService.deleteTopic("payments");
        assertEquals(1, stateMachine.topics().size());
    }
}
