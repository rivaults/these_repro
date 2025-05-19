package mrjobs;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import utils.SetupGenericsInput;
import writables.IdentifiedWritable;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Random;


public class SortMapper extends Mapper<NullWritable, IdentifiedWritable, LongWritable, IdentifiedWritable> {

    private final Random r = new Random();
    private final LongWritable outK = new LongWritable();

    @Override
    protected void setup(Context context) {
        try {
            SetupGenericsInput.setup(context.getConfiguration());
        } catch (IllegalAccessException | InstantiationException | NoSuchMethodException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void map(NullWritable k, IdentifiedWritable v, Context context) throws IOException, InterruptedException {
        outK.set(v.getId());
        context.write(outK, v);
    }
}
