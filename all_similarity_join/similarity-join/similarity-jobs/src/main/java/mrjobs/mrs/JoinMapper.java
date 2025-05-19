package mrjobs.mrs;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import utils.GenericHistogramsReader;
import utils.SetupTools;
import writables.AdaptiveKeyWritable;
import writables.IdentifiedWritable;
import writables.histogram.HistogramWritable;
import writables.partitionned.GenericKeysObjectWritable;
import writables.tensoring.LSHPartitioner;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

import static utils.SetupTools.*;

public class JoinMapper extends Mapper<NullWritable, IdentifiedWritable, AdaptiveKeyWritable, GenericKeysObjectWritable> {

    private LSHPartitioner partitioner;
    private final Map<Long, HistogramWritable> histograms = new HashMap<>();
    private final Random r = new Random(42);
    private final AdaptiveKeyWritable outK = new AdaptiveKeyWritable();
    private GenericHistogramsReader<HistogramWritable> reader;
    private HistogramWritable[] currHists;
    private GenericKeysObjectWritable outV;

    @Override
    public void setup(Context context) throws IOException {
        try {
            SetupTools.setup(context.getConfiguration());
            partitioner = SetupTools.readPartitionners(context, true);
            histograms.clear();
            reader = new GenericHistogramsReader<>();
            reader.initialize(context, histograms);
            currHists = new HistogramWritable[NB_ITERATIONS];
            outV = FACTORY.getObjectKeysInstance();
        } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            e.printStackTrace();
        }
    }

    public void map(NullWritable n, IdentifiedWritable v, Context context)
            throws IOException, InterruptedException {
        outV.set(v);
        outV.setEstimators(SKETCH.digest(v));
        partitioner.digest(v);
        for(int i = 0; i < NB_ITERATIONS; ++i) {
            outV.setKey(i, partitioner.next());
            currHists[i] = histograms.get(outV.getKey(i));
            if (currHists[i] == null)
                outV.setKey(i, null);
        }
        for (int i = 0; i < NB_ITERATIONS; ++i) {
            if (outV.getKey(i) != null)
                sendKeyValue(context, outV, outV.getKey(i), currHists[i]);
        }
        reader.update(context, histograms);
    }

    private void sendKeyValue(Context context, GenericKeysObjectWritable outV, long key, HistogramWritable h)
            throws IOException, InterruptedException{
        int buckets = h.getNbBuckets();
        if (buckets == 1) {
            outK.setAll(key);
            context.write(outK, outV);
        } else {
            int myBucket = r.nextInt(buckets);
            int height = buckets / 2;
            outK.setAll(key,
                    (short) (h.getIdReducer() + myBucket),
                    (byte) 0);
            context.write(outK, outV);
            for (int b = 0; b < height; ++b) {
                int nextBucket = (myBucket + b + 1) % buckets;
                outK.setAll(key, (short) (h.getIdReducer() + nextBucket), (byte) (1 + b));
                context.write(outK, outV);
            }
        }
    }
}
