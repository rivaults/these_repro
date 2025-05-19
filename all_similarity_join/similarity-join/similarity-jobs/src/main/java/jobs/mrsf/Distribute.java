package jobs.mrsf;

import mrjobs.mrsf.DistributeCombiner;
import mrjobs.mrsf.DistributeMapper;
import mrjobs.mrsf.DistributePartitioner;
import mrjobs.mrsf.DistributeReducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import utils.SetupTools;
import writables.partitionned.LongListWritable;
import writables.histogram.IntPairWritable;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

public class Distribute {

    private static final String PATH_EXP_DISTRIBUTE = "distribute-filtered";

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
        GenericOptionsParser parser = new GenericOptionsParser(args);
        String[] remaining = parser.getRemainingArgs();
        parser.getConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");
        Job job = Job.getInstance(parser.getConfiguration(), PATH_EXP_DISTRIBUTE);
        SetupTools.setup(job.getConfiguration());

        job.setJarByClass(Distribute.class);
        job.setMapperClass(DistributeMapper.class);
        job.setCombinerClass(DistributeCombiner.class);
        job.setPartitionerClass(DistributePartitioner.class);
        job.setReducerClass(DistributeReducer.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);

        job.setMapOutputKeyClass(IntPairWritable.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(LongListWritable.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(remaining[0]));
        FileOutputFormat.setOutputPath(job, new Path(remaining[1]));

        SetupTools.setCompressOutGzip(job);
        SetupTools.setNbReducers(job, remaining[2]);

        SetupTools.run(job, PATH_EXP_DISTRIBUTE);
    }
}
