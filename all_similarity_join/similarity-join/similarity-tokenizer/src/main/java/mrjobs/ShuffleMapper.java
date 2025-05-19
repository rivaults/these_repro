package mrjobs;

import utils.SetupGenericsInput;
import writables.IdentifiedWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Random;


public class ShuffleMapper extends Mapper<NullWritable, IdentifiedWritable, IntWritable, IdentifiedWritable> {

    private final Random r = new Random();
    private final IntWritable outK = new IntWritable();

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
        outK.set(r.nextInt());
        context.write(outK, v);
    }
}
