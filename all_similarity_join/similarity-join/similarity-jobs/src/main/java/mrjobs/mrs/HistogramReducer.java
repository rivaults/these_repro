package mrjobs.mrs;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.VLongWritable;
import utils.SetupTools;
import org.apache.hadoop.mapreduce.Reducer;
import writables.histogram.FrequencyWritable;
import writables.histogram.HistogramPartitionsWritable;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;
import java.util.Random;

public class HistogramReducer extends Reducer<LongWritable, FrequencyWritable, LongWritable, HistogramPartitionsWritable> {

    private final HistogramPartitionsWritable outV = new HistogramPartitionsWritable();
    private final Random r = new Random();
    private final FrequencyWritable tmp = new FrequencyWritable();

    @Override
    public void setup(Context context){
        try {
            SetupTools.setup(context.getConfiguration());
        } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    protected void reduce(LongWritable key, Iterable<FrequencyWritable> values, Context context)
            throws IOException, InterruptedException {
        tmp.clear();
        for (FrequencyWritable current : values){
            tmp.union(current);
            tmp.add(current);
        }
        if (tmp.shouldKeep()) {
            outV.setBucketsNbByFrequency(tmp.getFrequency());
            outV.setIdReducer(r.nextInt());
            outV.getPartitions().clear();
            outV.getPartitions().addAll(tmp.getPartitions());
            context.write(key, outV);
        }
    }
}
