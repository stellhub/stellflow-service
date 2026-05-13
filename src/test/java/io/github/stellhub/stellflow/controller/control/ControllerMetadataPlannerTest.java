package io.github.stellhub.stellflow.controller.control;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * ControllerMetadataPlanner 测试。
 */
class ControllerMetadataPlannerTest {

    /**
     * 验证关闭 unclean election 时不会选非 ISR 副本。
     */
    @Test
    void shouldKeepCurrentLeaderWhenOnlyNonIsrReplicaIsAliveAndUncleanDisabled() {
        ControllerMetadataPlanner planner = new ControllerMetadataPlanner();
        ControllerPartitionMetadata current =
                ControllerMetadataStateMachine.partition(
                        "orders", 0, 1, 3, List.of(1, 2), List.of(1), null, null);

        ControllerPartitionMetadata next =
                planner.reconcilePartition(current, List.of(fenced(1), alive(2)), false);

        assertEquals(1, next.leaderId());
        assertEquals(List.of(1), next.isrNodes());
    }

    /**
     * 验证开启 unclean election 时允许非 ISR 副本接任。
     */
    @Test
    void shouldElectNonIsrReplicaWhenUncleanEnabled() {
        ControllerMetadataPlanner planner = new ControllerMetadataPlanner();
        ControllerPartitionMetadata current =
                ControllerMetadataStateMachine.partition(
                        "orders", 0, 1, 3, List.of(1, 2), List.of(1), null, null);

        ControllerPartitionMetadata next =
                planner.reconcilePartition(current, List.of(fenced(1), alive(2)), true);

        assertEquals(2, next.leaderId());
        assertEquals(List.of(2), next.isrNodes());
    }

    private static BrokerRegistrationMetadata alive(int brokerId) {
        return new BrokerRegistrationMetadata(
                brokerId,
                "stellflow://127.0.0.1:" + (9092 + brokerId),
                "127.0.0.1",
                9092 + brokerId,
                System.currentTimeMillis(),
                false);
    }

    private static BrokerRegistrationMetadata fenced(int brokerId) {
        return new BrokerRegistrationMetadata(
                brokerId,
                "stellflow://127.0.0.1:" + (9092 + brokerId),
                "127.0.0.1",
                9092 + brokerId,
                System.currentTimeMillis(),
                true);
    }
}
