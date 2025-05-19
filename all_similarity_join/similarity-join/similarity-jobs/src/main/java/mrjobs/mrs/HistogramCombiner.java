package mrjobs.mrs;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import writables.histogram.FrequencyWritable;

import java.io.IOException;
import java.util.Iterator;

public class HistogramCombiner extends Reducer<LongWritable, FrequencyWritable, LongWritable, FrequencyWritable> {

    @Override
    protected void reduce(LongWritable key, Iterable<FrequencyWritable> values, Context context)
            throws IOException, InterruptedException {
        Iterator<FrequencyWritable> ite = values.iterator();
        FrequencyWritable freq = new FrequencyWritable(ite.next());
        FrequencyWritable current;
        while (ite.hasNext()){
            current = ite.next();
            freq.union(current);
            freq.add(current);
        }
        context.write(key, freq);
    }
}
