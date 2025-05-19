package jobs.mrs;

import mrjobs.mrs.HistogramCombiner;
import mrjobs.mrs.HistogramMapper;
import mrjobs.mrs.HistogramReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.VLongWritable;
import utils.SetupTools;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import writables.histogram.FrequencyWritable;
import writables.histogram.HistogramPartitionsWritable;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

public class ComputeHistogram {

    private static final String PATH_EXP_COMPUTE_H = "histogram";

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
        GenericOptionsParser parser = new GenericOptionsParser(args);
        String[] remaining = parser.getRemainingArgs();

        Job job = Job.getInstance(parser.getConfiguration(), PATH_EXP_COMPUTE_H);
        SetupTools.setup(job.getConfiguration());
        SetupTools.writePartitionners(job);
        job.setJarByClass(ComputeHistogram.class);

        job.setMapperClass(HistogramMapper.class);
        job.setCombinerClass(HistogramCombiner.class);
        job.setReducerClass(HistogramReducer.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(FrequencyWritable.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(HistogramPartitionsWritable.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        SetupTools.setCompress(job);

        FileInputFormat.addInputPath(job, new Path(remaining[0]));
        FileOutputFormat.setOutputPath(job, new Path(remaining[1]));
        SetupTools.setNbReducers(job, remaining[2]);

        SetupTools.run(job, PATH_EXP_COMPUTE_H);
    }
}
