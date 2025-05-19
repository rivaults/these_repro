package mrjobs.mrs;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import utils.Hash;
import utils.SetupTools;
import writables.histogram.HistogramPartitionsWritable;
import writables.histogram.IntPairWritable;
import writables.histogram.LongHistWritable;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

public class DistributeMapper extends Mapper<LongWritable, HistogramPartitionsWritable, IntPairWritable, LongHistWritable> {

    private final IntPairWritable outK = new IntPairWritable();
    private final LongHistWritable outV = new LongHistWritable();

    @Override
    public void setup(Context context) {
        try {
            SetupTools.setup(context.getConfiguration());
        } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void map(LongWritable k, HistogramPartitionsWritable v, Context context) throws IOException, InterruptedException {
        outV.setKey(k.get());
        outV.setHistogram(v);
        for(int p : v.getPartitions()){
            outK.setSplitId(Hash.getSplitId(p));
            outK.setHistId(Hash.getHistId(p));
            context.write(outK, outV);
        }
    }
}
