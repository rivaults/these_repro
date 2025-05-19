package mrjobs.mrs;

import writables.AdaptiveKeyWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import writables.partitionned.GenericKeysObjectWritable;

public class JoinPartitioner extends Partitioner<AdaptiveKeyWritable, GenericKeysObjectWritable> {

    @Override
    public int getPartition(AdaptiveKeyWritable key, GenericKeysObjectWritable value, int numPartitions) {
        if (key.isDistributed())
            return (key.getReducerId() & Integer.MAX_VALUE) % numPartitions;
        return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
