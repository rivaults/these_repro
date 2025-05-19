package mrjobs.mrsf;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import utils.SetupTools;
import writables.partitionned.LongListWritable;
import writables.histogram.IntPairWritable;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.HashSet;

import static experiments.RES_COUNTERS.DISTRIBUTED_FILTERED;
import static experiments.RES_COUNTERS.FALSE_POSITIVE;
import static utils.SetupTools.COUNTERS_ENABLED;

public class DistributeReducer extends Reducer<IntPairWritable, LongWritable, IntWritable, LongListWritable>{

    private final IntWritable outK = new IntWritable();
    private final LongListWritable outV = new LongListWritable();
    private final HashSet<Long> transfer = new HashSet<>();

    @Override
    public void setup(Context context) {
        try {
            SetupTools.setup(context.getConfiguration());
        } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void reduce(IntPairWritable key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
        transfer.clear();
        outV.clear();
        outK.set(key.getHistId());
        for(LongWritable e : values){
            transfer.add(e.get());
        }
        outV.addAll(transfer);
        context.write(outK, outV);
        if (COUNTERS_ENABLED)
            context.getCounter(DISTRIBUTED_FILTERED).increment(transfer.size());
    }
}
