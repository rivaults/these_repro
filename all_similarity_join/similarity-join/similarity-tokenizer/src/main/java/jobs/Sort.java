package jobs;

import mrjobs.ShuffleMapper;
import mrjobs.ShuffleReducer;
import mrjobs.SortMapper;
import mrjobs.SortReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import utils.SetupGenericsInput;
import utils.SetupTokenizer;

import static utils.SetupGenericsInput.OBJECT_CLS;

public class Sort {
    public static void main(String[] args) throws Exception {
        GenericOptionsParser parser = new GenericOptionsParser(args);
        String[] remaining = parser.getRemainingArgs();
        Job job = Job.getInstance(parser.getConfiguration(), "sort");
        SetupGenericsInput.setup(job.getConfiguration());

        job.setJarByClass(Sort.class);
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        SetupTokenizer.prepare(job);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(OBJECT_CLS);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(OBJECT_CLS);

        SetupTokenizer.run(remaining, job);
    }
}
