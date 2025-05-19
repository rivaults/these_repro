package mrjobs.mrs;

import org.apache.hadoop.io.LongWritable;
import utils.SetupTools;
import org.apache.hadoop.io.NullWritable;
import writables.IdentifiedWritable;
import writables.histogram.FrequencyWritable;
import org.apache.hadoop.mapreduce.Mapper;
import writables.tensoring.LSHPartitioner;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import static utils.SetupTools.NB_ITERATIONS;
import static utils.SetupTools.SIZE_GROUP;

public class HistogramMapper extends Mapper<NullWritable, IdentifiedWritable, LongWritable, FrequencyWritable> {

    private LSHPartitioner partitioner;
    private final LongWritable outK = new LongWritable();
    private final FrequencyWritable outV = new FrequencyWritable();
    private int group;
    private int counter;

    @Override
    public void setup(Context context) throws IOException {
        try {
            SetupTools.setup(context.getConfiguration());
            partitioner = SetupTools.readPartitionners(context, false);
        } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
        counter = 0;
        group = 0;
    }

    @Override
    public void map(NullWritable n, IdentifiedWritable v, Context context)
            throws IOException, InterruptedException {
        outV.setAll(SetupTools.getIdSplit(context.getTaskAttemptID()), group);
        partitioner.digest(v);
        for(int i = 0; i < NB_ITERATIONS; ++i) {
            outK.set(partitioner.next());
            context.write(outK, outV);
        }
        counter++;
        if (counter > SIZE_GROUP) {
            group++;
            counter = 0;
        }
    }
}
