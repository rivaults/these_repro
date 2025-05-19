package mrjobs;

import utils.SetupGenericsInput;
import writables.IdentifiedWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

public class ShuffleReducer extends Reducer<IntWritable, IdentifiedWritable, NullWritable, IdentifiedWritable> {

    @Override
    protected void setup(Context context) {
        try {
            SetupGenericsInput.setup(context.getConfiguration());
        } catch (IllegalAccessException | InstantiationException | NoSuchMethodException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void reduce(IntWritable key, Iterable<IdentifiedWritable> values, Context context) throws IOException, InterruptedException {
        for(IdentifiedWritable s : values) {
            context.write(NullWritable.get(), s);
        }
    }
}
