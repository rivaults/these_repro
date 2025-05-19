package jobs;

import mrjobs.UniqMapper;
import mrjobs.UniqReducer;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import utils.SetupTokenizer;
import utils.SetupGenericsInput;

import static utils.SetupGenericsInput.*;

public class Uniq {
    public static void main(String[] args) throws Exception {
        if (args.length < 2)
            throw new Exception("parameters needed: input output");
        GenericOptionsParser parser = new GenericOptionsParser(args);
        String[] remaining = parser.getRemainingArgs();
        Job job = Job.getInstance(parser.getConfiguration(), "uniq");
        SetupGenericsInput.setup(job.getConfiguration());

        job.setJarByClass(Uniq.class);
        job.setMapperClass(UniqMapper.class);
        job.setReducerClass(UniqReducer.class);

        job.setInputFormatClass(READER_OBJECT_CLS);
        SetupTokenizer.prepare(job);

        job.setMapOutputKeyClass(BytesWritable.class);
        job.setMapOutputValueClass(OBJECT_CLS);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(OBJECT_CLS);

        SetupTokenizer.run(remaining, job);
    }


}
