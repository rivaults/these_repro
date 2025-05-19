package mrjobs.mrsf;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import writables.histogram.IntPairWritable;

import java.io.IOException;
import java.util.HashSet;

public class DistributeCombiner extends Reducer<IntPairWritable, LongWritable, IntPairWritable, LongWritable> {

    private final HashSet<Long> transfer = new HashSet<>();
    private final LongWritable outV = new LongWritable();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void reduce(IntPairWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        transfer.clear();
        for(LongWritable e : values){
            transfer.add(e.get());
        }
        for(long k : transfer){
            outV.set(k);
            context.write(key, outV);
        }
    }
}
