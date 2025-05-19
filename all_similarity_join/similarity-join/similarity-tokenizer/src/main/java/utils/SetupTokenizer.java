package utils;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class SetupTokenizer {

    public static void run(String[] remaining, Job job) throws java.io.IOException, InterruptedException, ClassNotFoundException {
        FileInputFormat.addInputPath(job, new Path(remaining[0]));
        FileOutputFormat.setOutputPath(job, new Path(remaining[1]));
        job.setNumReduceTasks(Integer.parseInt(remaining[2]));
        if (job.waitForCompletion(true)){
            System.exit(0);
        }
        System.exit(1);
    }

    public static void prepare(Job job) {
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        if (job.getConfiguration().getBoolean("compress.snappy", false)) {
            job.getConfiguration().set("mapreduce.map.output.compress", "true");
            job.getConfiguration().set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
            SequenceFileOutputFormat.setCompressOutput(job, true);
            SequenceFileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
            SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
        }
    }
}
