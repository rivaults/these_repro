package mrjobs.mrs;

import org.apache.hadoop.mapreduce.Partitioner;
import writables.histogram.IntPairWritable;
import writables.histogram.LongHistWritable;

public class DistributePartitioner extends Partitioner<IntPairWritable, LongHistWritable> {

    @Override
    public int getPartition(IntPairWritable key, LongHistWritable value, int numPartitions) {
        return key.getSplitId() % numPartitions;
    }
}