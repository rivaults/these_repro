package mrjobs;

import org.apache.hadoop.io.BytesWritable;
import utils.SetupGenericsInput;
import writables.IdentifiedWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

public class UniqReducer extends Reducer<BytesWritable, IdentifiedWritable, NullWritable, IdentifiedWritable> {

    @Override
    protected void setup(Context context) {
        try {
            SetupGenericsInput.setup(context.getConfiguration());
        } catch (IllegalAccessException | InstantiationException | NoSuchMethodException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void reduce(BytesWritable key, Iterable<IdentifiedWritable> values, Context context) throws IOException, InterruptedException {
        context.write(NullWritable.get(), values.iterator().next());
    }
}
