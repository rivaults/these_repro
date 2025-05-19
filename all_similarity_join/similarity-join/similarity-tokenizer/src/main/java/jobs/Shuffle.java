package jobs;

import mrjobs.ShuffleReducer;
import mrjobs.ShuffleMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import utils.SetupTokenizer;
import utils.SetupGenericsInput;

import static utils.SetupGenericsInput.OBJECT_CLS;

public class Shuffle {
    public static void main(String[] args) throws Exception {
        GenericOptionsParser parser = new GenericOptionsParser(args);
        String[] remaining = parser.getRemainingArgs();
        Job job = Job.getInstance(parser.getConfiguration(), "shuffle");
        SetupGenericsInput.setup(job.getConfiguration());

        job.setJarByClass(Shuffle.class);
        job.setMapperClass(ShuffleMapper.class);
        job.setReducerClass(ShuffleReducer.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        SetupTokenizer.prepare(job);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(OBJECT_CLS);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(OBJECT_CLS);

        SetupTokenizer.run(remaining, job);
    }
}
