package com.example.migration.batch;

import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;

import java.util.HashMap;
import java.util.Map;

/**
 * ORA_HASH-based ROWID partitioner.
 *
 * This partitioner does NOT compute ROWID ranges.
 * Instead, it assigns each partition a partitionIndex and gridSize.
 *
 * The reader must use:
 *   MOD(ORA_HASH(ROWID), gridSize) = partitionIndex
 *
 * This approach is:
 * - Deterministic
 * - Parallel-safe
 * - Restart-safe
 * - Suitable when no numeric primary key exists
 */
public class RowIdPartitioner implements Partitioner {

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {

        Map<String, ExecutionContext> partitionMap = new HashMap<>();

        for (int partitionIndex = 0; partitionIndex < gridSize; partitionIndex++) {
            ExecutionContext context = new ExecutionContext();
            context.putInt("partitionIndex", partitionIndex);
            context.putInt("gridSize", gridSize);
            partitionMap.put("partition-" + partitionIndex, context);
        }

        return partitionMap;
    }
}