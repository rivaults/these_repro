package mrjobs.mrs;

import org.apache.hadoop.io.LongWritable;
import utils.SetupTools;
import writables.AdaptiveKeyWritable;
import org.apache.hadoop.mapreduce.Reducer;
import writables.estimator.Sketching;
import writables.partitionned.GenericKeysObjectWritable;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Objects;

import static experiments.RES_COUNTERS.*;
import static utils.SetupTools.*;

public class JoinReducer extends Reducer<AdaptiveKeyWritable, GenericKeysObjectWritable, LongWritable, LongWritable> {

    private final ArrayList<GenericKeysObjectWritable> sets = new ArrayList<>();
    private final LongWritable writerK = new LongWritable();
    private final LongWritable writerV = new LongWritable();

    private Long currentKey = null;
    private Integer currentReducerId = null;

    @Override
    protected void setup(Context context) throws IOException {
        try {
            SetupTools.setup(context.getConfiguration());
            Sketching.setup(context.getConfiguration());
        } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            e.printStackTrace();
        }
    }

    private void reset(long key, int reducerId) {
        sets.clear();
        currentKey = key;
        currentReducerId = reducerId;
    }

    public void reduce(AdaptiveKeyWritable key, Iterable<GenericKeysObjectWritable> collection, Context context)
            throws IOException, InterruptedException {
        if (key.isDistributed()) {
            if (key.isPrimaryKey()) {
                reset(key.getKey(), key.getReducerId());
                brute(collection, context, true);
            }
            else if (currentKey == key.getKey() &&
                    currentReducerId == key.getReducerId()) {
                brute(collection, context, false);
            }
        }
        else {
            if (!Objects.equals(currentKey, key.getKey()))
                reset(key.getKey(), Integer.MAX_VALUE);
            brute(collection, context, true);
        }
    }

    private void brutePair(Context context,
                           GenericKeysObjectWritable x,
                           GenericKeysObjectWritable y) throws IOException, InterruptedException {
        boolean isColliding = isColliding(x, y, currentKey);
        boolean sketchFilter = SKETCH.compareSketches(x.getEstimators(), y.getEstimators());
        if (isColliding && sketchFilter){
            if (DISTANCE.verify(x.get(), y.get())){
                writerK.set(x.get().getId());
                writerV.set(y.get().getId());
                context.write(writerK, writerV);
            }
            else if (COUNTERS_ENABLED){
                context.getCounter(FALSE_POSITIVE).increment(1);
            }
        }else if (isColliding){
            context.getCounter(SKETCH_FILTER).increment(1);
        }
    }

    private void brute(Iterable<GenericKeysObjectWritable> collection,
                       Context context,
                       boolean add) throws IOException, InterruptedException {
        for (GenericKeysObjectWritable t1 : collection) {
            for (GenericKeysObjectWritable t2 : sets) {
                brutePair(context, t1, t2);
            }
            if (add)
                sets.add(t1.copy());
        }
    }

    private boolean isColliding(GenericKeysObjectWritable t1,
                                GenericKeysObjectWritable t2,
                                long currentVal){
        long min = currentVal;
        for(int i = 0; i < NB_ITERATIONS; ++i)
            if (t1.getKey(i) != null && Objects.equals(t1.getKey(i), t2.getKey(i)) && t1.getKey(i) < min)
                min = t1.getKey(i);
        return min == currentVal;
    }
}
