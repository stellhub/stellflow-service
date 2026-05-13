package io.github.stellhub.stellflow.coordinator;

import io.github.stellhub.stellflow.storage.log.TopicPartition;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Range 分区分配策略。
 */
public class RangePartitionAssignor {

    /**
     * 按成员顺序均匀分配分区。
     */
    public List<PartitionAssignment> assign(List<String> memberIds, List<TopicPartition> partitions) {
        List<String> sortedMembers = memberIds.stream().sorted().toList();
        List<TopicPartition> sortedPartitions =
                partitions.stream()
                        .sorted(Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition))
                        .toList();
        Map<String, List<TopicPartition>> assignments = new LinkedHashMap<>();
        for (String memberId : sortedMembers) {
            assignments.put(memberId, new ArrayList<>());
        }
        if (sortedMembers.isEmpty()) {
            return List.of();
        }
        for (int index = 0; index < sortedPartitions.size(); index++) {
            String memberId = sortedMembers.get(index % sortedMembers.size());
            assignments.get(memberId).add(sortedPartitions.get(index));
        }
        return assignments.entrySet().stream()
                .map(entry -> new PartitionAssignment(entry.getKey(), List.copyOf(entry.getValue())))
                .toList();
    }
}
