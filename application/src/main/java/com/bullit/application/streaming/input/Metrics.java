package com.bullit.application.streaming.input;

import org.apache.kafka.common.TopicPartition;
import java.util.Map;

public class Metrics {
    public record StreamMetrics(
            String topic,
            int partitionsKnown,
            int pausedPartitions,
            long bufferedRecordsTotal,
            long bufferedRecordsMaxPerPartition,
            Map<TopicPartition, PartitionMetrics> byPartition
    ) {
    }

    public record PartitionMetrics(
            int bufferedRecords,
            int remainingCapacity,
            boolean paused,
            Long nextOffset
    ) {
    }
}
