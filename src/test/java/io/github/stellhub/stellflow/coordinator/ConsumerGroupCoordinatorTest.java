package io.github.stellhub.stellflow.coordinator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.stellhub.stellflow.network.protocol.ErrorCode;
import io.github.stellhub.stellflow.storage.log.TopicPartition;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * ConsumerGroupCoordinator 测试。
 */
class ConsumerGroupCoordinatorTest {

    @TempDir private Path tempDir;

    /**
     * 验证成员超时后会被驱逐并触发代次变更。
     */
    @Test
    void shouldEvictExpiredMembers() {
        ConsumerGroupCoordinator coordinator =
                new ConsumerGroupCoordinator(new OffsetStore(tempDir.resolve("offsets.snapshot")));
        var join =
                coordinator.joinGroup("group-a", "member-a", "client-a", "127.0.0.1", 10);

        int removed = coordinator.evictExpiredMembers(System.currentTimeMillis() + 100);
        ErrorCode heartbeat =
                coordinator.heartbeat("group-a", join.generationId(), join.memberId());

        assertEquals(1, removed);
        assertEquals(ErrorCode.UNKNOWN_SERVER_ERROR, heartbeat);
    }

    /**
     * 验证 Range 分配策略会覆盖全部分区。
     */
    @Test
    void shouldAssignPartitionsByRangeStrategy() {
        ConsumerGroupCoordinator coordinator =
                new ConsumerGroupCoordinator(new OffsetStore(tempDir.resolve("offsets.snapshot")));
        coordinator.joinGroup("group-a", "member-a", "client-a", "127.0.0.1", 30000);
        coordinator.joinGroup("group-a", "member-b", "client-b", "127.0.0.1", 30000);
        coordinator.updateAssignablePartitions(
                "group-a",
                List.of(
                        new TopicPartition("orders", 0),
                        new TopicPartition("orders", 1),
                        new TopicPartition("orders", 2)));

        List<TopicPartition> memberA = coordinator.assignment("group-a", "member-a");
        List<TopicPartition> memberB = coordinator.assignment("group-a", "member-b");

        assertEquals(3, memberA.size() + memberB.size());
        assertTrue(memberA.stream().noneMatch(memberB::contains));
    }
}
