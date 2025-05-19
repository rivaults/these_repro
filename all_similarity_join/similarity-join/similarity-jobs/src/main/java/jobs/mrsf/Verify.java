package jobs.mrsf;

import mrjobs.mrsf.VerifyMapper;
import mrjobs.mrsf.VerifyReducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import utils.SetupTools;
import writables.AdaptiveKeyWritable;
import writables.IdentifiedWritable;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import static utils.SetupTools.FACTORY;
import static utils.SetupTools.PATH_PARTITIONERS;

public class Verify {

    private static final String PATH_EXP_VERIFY = "verify";

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
        GenericOptionsParser parser = new GenericOptionsParser(args);
        String[] remaining = parser.getRemainingArgs();

        Job job = Job.getInstance(parser.getConfiguration(), PATH_EXP_VERIFY);
        SetupTools.setup(job.getConfiguration());
        job.addCacheFile(PATH_PARTITIONERS.toUri());

        job.setJarByClass(Verify.class);
        job.setMapperClass(VerifyMapper.class);
        job.setReducerClass(VerifyReducer.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapOutputKeyClass(AdaptiveKeyWritable.class);
        job.setMapOutputValueClass(FACTORY.getObjectClass());

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        SetupTools.setCompressAllGzip(job);

        FileInputFormat.addInputPath(job, new Path(remaining[0]));
        FileOutputFormat.setOutputPath(job, new Path(remaining[1]));
        SetupTools.addCacheFiles(job, remaining[2]);

        SetupTools.setNbReducers(job, remaining[3]);
        SetupTools.run(job, PATH_EXP_VERIFY);
    }
}
