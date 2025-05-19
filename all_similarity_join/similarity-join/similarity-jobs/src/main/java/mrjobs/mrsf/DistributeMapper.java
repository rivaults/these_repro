package mrjobs.mrsf;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import utils.Hash;
import utils.SetupTools;
import writables.partitionned.PartitionWritable;
import writables.histogram.IntPairWritable;
import writables.partitionned.LocalizedIdWritable;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;


public class DistributeMapper extends Mapper<LocalizedIdWritable, LocalizedIdWritable, IntPairWritable, LongWritable> {

    private final IntPairWritable outK = new IntPairWritable();
    private final LongWritable outV = new LongWritable();

    @Override
    public void setup(Context context) {
        try {
            SetupTools.setup(context.getConfiguration());
        } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void map(LocalizedIdWritable k, LocalizedIdWritable v, Context context) throws IOException, InterruptedException {
        outK.setSplitId(k.getSplitId());
        outK.setHistId(k.getLocalization());
        outV.set(k.getId());
        context.write(outK, outV);
        outK.setSplitId(v.getSplitId());
        outK.setHistId(v.getLocalization());
        outV.set(k.getId());
        context.write(outK, outV);
    }
}
