package mrjobs.mrsf;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import utils.SetupTools;
import writables.AdaptiveKeyWritable;
import writables.IdentifiedWritable;
import writables.partitionned.LinksIdentifiedWritable;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;

import static experiments.RES_COUNTERS.FALSE_POSITIVE;
import static utils.SetupTools.*;

public class VerifyReducer extends Reducer<AdaptiveKeyWritable, IdentifiedWritable, LongWritable, LongWritable> {

    private final LongWritable writerK = new LongWritable();
    private final LongWritable writerV = new LongWritable();

    private Long currentKey = null;
    private IdentifiedWritable from;

    @Override
    protected void setup(Context context) throws IOException {
        try {
            SetupTools.setup(context.getConfiguration());
        } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            e.printStackTrace();
        }
    }

    public void reduce(AdaptiveKeyWritable key, Iterable<IdentifiedWritable> collection, Context context)
            throws IOException, InterruptedException {
        if (key.isPrimaryKey()) {
            currentKey = key.getKey();
            from = collection.iterator().next().copy();
        }
        else if (currentKey == key.getKey()) {
            for (IdentifiedWritable to : collection) {
                brutePair(context, from, to);
            }
        }
    }

    private void brutePair(Context context,
                           IdentifiedWritable x,
                           IdentifiedWritable y) throws IOException, InterruptedException {
        if (DISTANCE.verify(x, y)){
            writerK.set(x.getId());
            writerV.set(y.getId());
            context.write(writerK, writerV);
        }
        else if (COUNTERS_ENABLED){
            context.getCounter(FALSE_POSITIVE).increment(1);
        }
    }
}
