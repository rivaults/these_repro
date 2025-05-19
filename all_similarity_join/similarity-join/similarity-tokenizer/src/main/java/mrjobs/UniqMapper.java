package mrjobs;

import org.apache.hadoop.io.LongWritable;
import utils.SetupGenericsInput;
import writables.IdentifiedWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

public class UniqMapper extends Mapper<NullWritable, IdentifiedWritable, BytesWritable, IdentifiedWritable> {

    private final BytesWritable outK = new BytesWritable();

    @Override
    protected void setup(Context context) {
        try {
            SetupGenericsInput.setup(context.getConfiguration());
        } catch (IllegalAccessException | InstantiationException | NoSuchMethodException | InvocationTargetException e) {
            throw new RuntimeException("SETUP ERROR : " + e.getMessage());
        }
    }

    @Override
    protected void map(NullWritable key, IdentifiedWritable value, Context context) throws IOException, InterruptedException {
        outK.set(value.getBytes(), 0, value.getLength());
        context.write(outK, value);
    }
}