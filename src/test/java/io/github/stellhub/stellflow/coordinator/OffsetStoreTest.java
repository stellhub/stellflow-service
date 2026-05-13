package io.github.stellhub.stellflow.coordinator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * OffsetStore 测试。
 */
class OffsetStoreTest {

    @TempDir private Path tempDir;

    /**
     * 验证 offset 提交后可从磁盘恢复。
     */
    @Test
    void shouldRecoverCommittedOffsetFromDisk() {
        Path snapshot = tempDir.resolve("offsets.snapshot");
        OffsetStore first = new OffsetStore(snapshot);
        first.commit("group-a", "orders", 0, 12L, "manual");

        OffsetStore recovered = new OffsetStore(snapshot);

        var offset = recovered.fetch("group-a", "orders", 0);
        assertTrue(offset.isPresent());
        assertEquals(12L, offset.orElseThrow().offset());
        assertEquals("manual", offset.orElseThrow().metadata());
    }
}
