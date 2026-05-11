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
        ControllerMetadataStateMachine stateMachine =
                new ControllerMetadataStateMachine(
                        assignmentRegistry, partitionControlRegistry, resultRegistry);

        stateMachine.registerBroker(1, "stellflow://127.0.0.1:9092", "127.0.0.1", 9092);
        stateMachine.registerBroker(2, "stellflow://127.0.0.1:9093", "127.0.0.1", 9093);
        stateMachine.registerBroker(3, "stellflow://127.0.0.1:9094", "127.0.0.1", 9094);

        stateMachine.replacePartitions(
                List.of(
                        ControllerMetadataStateMachine.partition(
                                "orders",
                                0,
                                1,
                                3,
                                List.of(1, 2),
                                List.of(1, 2),
                                null,
                                null),
                        ControllerMetadataStateMachine.partition(
                                "payments",
                                1,
                                1,
                                4,
                                List.of(1, 3),
                                List.of(1, 3),
                                2,
                                11L)));

        assertEquals(2, partitionControlRegistry.commands(1).size());
        assertEquals(1, partitionControlRegistry.commands(2).size());
        assertEquals(1, partitionControlRegistry.commands(3).size());
        assertEquals(1, assignmentRegistry.assignments(2).size());
        assertEquals("orders", assignmentRegistry.assignments(2).get(0).getTopic());
        assertEquals(1, assignmentRegistry.assignments(3).size());
        assertEquals("payments", assignmentRegistry.assignments(3).get(0).getTopic());
    }
}
