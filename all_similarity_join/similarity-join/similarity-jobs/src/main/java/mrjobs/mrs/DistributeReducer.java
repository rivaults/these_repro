package mrjobs.mrs;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import utils.SetupTools;
import writables.histogram.HistogramWritable;
import writables.histogram.IntPairWritable;
import writables.histogram.LongHistWritable;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

public class DistributeReducer extends Reducer<IntPairWritable, LongHistWritable, LongWritable, HistogramWritable>{

    private final LongWritable outK = new LongWritable();

    @Override
    public void setup(Context context) {
        try {
            SetupTools.setup(context.getConfiguration());
        } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void reduce(IntPairWritable key, Iterable<LongHistWritable> values, Context context)
            throws IOException, InterruptedException {
        for(LongHistWritable lh : values) {
            outK.set(lh.getKey());
            context.write(outK, lh.getHistogram());
        }
        outK.set(Long.MIN_VALUE);
        context.write(outK, new HistogramWritable());
    }
}
