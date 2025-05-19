package jobs.mrs;

import mrjobs.mrs.JoinPartitioner;
import mrjobs.mrs.JoinReducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import utils.SetupTools;
import mrjobs.mrs.JoinMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import writables.AdaptiveKeyWritable;


import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import static utils.SetupTools.*;

public class Join {

    private static final String PATH_EXP_JOIN = "join";

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
        GenericOptionsParser parser = new GenericOptionsParser(args);
        String[] remaining = parser.getRemainingArgs();

        Job job = Job.getInstance(parser.getConfiguration(), PATH_EXP_JOIN);
        SetupTools.setup(job.getConfiguration());
        job.addCacheFile(PATH_PARTITIONERS.toUri());

        job.setJarByClass(Join.class);
        job.setMapperClass(JoinMapper.class);
        job.setPartitionerClass(JoinPartitioner.class);
        job.setReducerClass(JoinReducer.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapOutputKeyClass(AdaptiveKeyWritable.class);
        job.setMapOutputValueClass(FACTORY.getObjectKeysClass());

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        SetupTools.setCompress(job);

        FileInputFormat.addInputPath(job, new Path(remaining[0]));
        FileOutputFormat.setOutputPath(job, new Path(remaining[1]));
        SetupTools.addCacheFiles(job, remaining[2]);

        SetupTools.setNbReducers(job, remaining[3]);
        SetupTools.run(job, PATH_EXP_JOIN);
    }
}
