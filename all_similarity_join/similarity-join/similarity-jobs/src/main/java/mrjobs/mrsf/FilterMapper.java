package mrjobs.mrsf;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import utils.GenericHistogramsReader;
import utils.Localizer;
import utils.SetupTools;
import writables.AdaptiveKeyWritable;
import writables.IdentifiedWritable;
import writables.histogram.HistogramWritable;
import writables.partitionned.LocalizedIdSketchedWritable;
import writables.tensoring.LSHPartitioner;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

import static utils.SetupTools.*;

public class FilterMapper extends Mapper<NullWritable, IdentifiedWritable, AdaptiveKeyWritable, LocalizedIdSketchedWritable> {

    private LSHPartitioner partitioner;
    private final Map<Long, HistogramWritable> histograms = new HashMap<>();
    private AdaptiveKeyWritable outK;
    private LocalizedIdSketchedWritable outV;
    private final Localizer localizer = new Localizer();
    private GenericHistogramsReader<HistogramWritable> reader;
    private final Random r = new Random();

    @Override
    public void setup(Context context) throws IOException {
        try {
            SetupTools.setup(context.getConfiguration());
            partitioner = SetupTools.readPartitionners(context, true);
            localizer.reset(SetupTools.getIdSplit(context.getTaskAttemptID()));
            histograms.clear();
            reader = new GenericHistogramsReader<>();
            reader.initialize(context, histograms);
            outK = new AdaptiveKeyWritable();
            outV = new LocalizedIdSketchedWritable();
        } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            e.printStackTrace();
        }
    }

    public void map(NullWritable n, IdentifiedWritable v, Context context)
            throws IOException, InterruptedException {
        localizer.next();
        partitioner.digest(v);
        boolean outVconfigured = false;
        for(int i = 0; i < NB_ITERATIONS; ++i) {
            long key = partitioner.next();
            HistogramWritable h = histograms.get(key);
            if (h != null) {
                if (!outVconfigured){
                    outVconfigured = true;
                    outV.setLocalizedId(localizer.getLocalizable(v.getId()));
                    outV.setEstimators(SKETCH.digest(v));
                    outV.setLength((short) v.getLength());
                }
                sendKeyValue(context, outV, key, h);
            }
        }
        reader.update(context, histograms);
    }

    private void sendKeyValue(Context context, LocalizedIdSketchedWritable outV, long key, HistogramWritable h)
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
