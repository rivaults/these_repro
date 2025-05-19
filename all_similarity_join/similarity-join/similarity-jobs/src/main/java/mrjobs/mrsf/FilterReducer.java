package mrjobs.mrsf;

import org.apache.hadoop.mapreduce.Reducer;
import utils.SetupTools;
import writables.AdaptiveKeyWritable;
import writables.estimator.Sketching;
import writables.partitionned.LocalizedIdSketchedWritable;
import writables.partitionned.LocalizedIdWritable;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Objects;
import java.util.Random;

import static experiments.RES_COUNTERS.*;
import static utils.SetupTools.*;

public class FilterReducer extends Reducer<AdaptiveKeyWritable, LocalizedIdSketchedWritable, LocalizedIdWritable, LocalizedIdWritable> {

    private final ArrayList<LocalizedIdSketchedWritable> sets = new ArrayList<>();
    private final Random r = new Random();
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

    public void reduce(AdaptiveKeyWritable key, Iterable<LocalizedIdSketchedWritable> collection, Context context)
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

    public void brute(Iterable<LocalizedIdSketchedWritable> collection, Context context, boolean toAdd)
            throws IOException, InterruptedException {
        for (LocalizedIdSketchedWritable x : collection) {
            for (LocalizedIdSketchedWritable y : sets) {
                brutePair(x, y, context);
            }
            if (toAdd)
                sets.add(x.copy());
        }
        sets.clear();
    }
    private void brutePair(LocalizedIdSketchedWritable x,
                           LocalizedIdSketchedWritable y,
                           Context context) throws IOException, InterruptedException {
        LocalizedIdWritable writerY;
        LocalizedIdWritable writerX;
        boolean sizeFilter = x.sizeFilter(y);
        boolean sketchFilter = SKETCH.compareSketches(x.getEstimators(), y.getEstimators());
        if (sizeFilter && sketchFilter) {
            if ((x.getLocalizedId().getId() & 1) == 0 && (y.getLocalizedId().getId() & 1) == 1){
                writerX = x.getLocalizedId();
                writerY = y.getLocalizedId();
            } else if ((x.getLocalizedId().getId() & 1) == 1 && (y.getLocalizedId().getId() & 1) == 0){
                writerX = y.getLocalizedId();
                writerY = x.getLocalizedId();
            } else if (x.getLocalizedId().getId() < y.getLocalizedId().getId()){
                writerX = x.getLocalizedId();
                writerY = y.getLocalizedId();
            } else{
                writerX = y.getLocalizedId();
                writerY = x.getLocalizedId();
            }
            context.write(writerX, writerY);
        }
        else if (COUNTERS_ENABLED && !sizeFilter)
            context.getCounter(SIZE_FILTER).increment(1);
        else if (COUNTERS_ENABLED && !sketchFilter)
            context.getCounter(SKETCH_FILTER).increment(1);
        else if (COUNTERS_ENABLED)
            context.getCounter(SKETCH_DEDUPLICATE).increment(1);
    }
}
