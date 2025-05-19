package mrjobs.mrsf;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import writables.histogram.IntPairWritable;

public class DistributePartitioner extends Partitioner<IntPairWritable, LongWritable> {

    @Override
    public int getPartition(IntPairWritable key, LongWritable value, int numPartitions) {
        return key.getSplitId() % numPartitions;
    }
}