package reader;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import writables.TrajectoryWritable;

import java.io.IOException;

public class TrajectoryInput extends FileInputFormat<NullWritable, TrajectoryWritable> {

    @Override
    public RecordReader<NullWritable, TrajectoryWritable> createRecordReader(InputSplit split,
                                                                             TaskAttemptContext context)
            throws IOException {
        TrajectoryReader reader = new TrajectoryReader();
        reader.initialize(split, context);
        return reader;
    }
}