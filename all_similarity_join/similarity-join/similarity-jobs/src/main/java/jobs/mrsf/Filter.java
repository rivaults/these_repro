package jobs.mrsf;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import utils.SetupTools;
import mrjobs.mrsf.FilterMapper;
import mrjobs.mrsf.FilterReducer;
import writables.AdaptiveKeyWritable;
import writables.partitionned.LocalizedIdSketchedWritable;
import writables.partitionned.LocalizedIdWritable;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

public class Filter {

    private static final String PATH_EXP_FILTER = "filter";

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
        GenericOptionsParser parser = new GenericOptionsParser(args);
        String[] remaining = parser.getRemainingArgs();

        Job job = Job.getInstance(parser.getConfiguration(), PATH_EXP_FILTER);
        SetupTools.setup(job.getConfiguration());
        SetupTools.writePartitionners(job);
        job.setJarByClass(Filter.class);

        job.setMapperClass(FilterMapper.class);
        job.setReducerClass(FilterReducer.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);

        job.setMapOutputKeyClass(AdaptiveKeyWritable.class);
        job.setMapOutputValueClass(LocalizedIdSketchedWritable.class);

        job.setOutputKeyClass(LocalizedIdWritable.class);
        job.setOutputValueClass(LocalizedIdWritable.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        SetupTools.setCompressAllGzip(job);

        FileInputFormat.addInputPath(job, new Path(remaining[0]));
        FileOutputFormat.setOutputPath(job, new Path(remaining[1]));
        SetupTools.addCacheFiles(job, remaining[2]);
        SetupTools.setNbReducers(job, remaining[3]);

        SetupTools.run(job, PATH_EXP_FILTER);
    }
}
