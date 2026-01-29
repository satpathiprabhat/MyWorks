
package com.example.migration.batch;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import java.util.HashMap;
import java.util.Map;

public class IdRangePartitioner implements Partitioner {

  private final long min;
  private final long max;
  private final int partitions;

  public IdRangePartitioner(long min,long max,int partitions){
    this.min=min; this.max=max; this.partitions=partitions;
  }

  public Map<String,ExecutionContext> partition(int gridSize){
    Map<String,ExecutionContext> map=new HashMap<>();
    long target=(max-min)/partitions;
    long start=min;
    long end=start+target;

    for(int i=0;i<partitions;i++){
      ExecutionContext ctx=new ExecutionContext();
      ctx.putLong("minId", start);
      ctx.putLong("maxId", i==partitions-1?max:end);
      map.put("partition"+i, ctx);
      start=end+1;
      end+=target;
    }
    return map;
  }
}
