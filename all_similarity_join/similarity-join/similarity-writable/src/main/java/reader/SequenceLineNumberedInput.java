package reader;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import writables.SequenceWritable;

import java.io.IOException;

public class SequenceLineNumberedInput extends FileInputFormat<NullWritable, SequenceWritable> {

    @Override
    public RecordReader<NullWritable, SequenceWritable> createRecordReader(InputSplit split,
                                                                      TaskAttemptContext context)
            throws IOException {
        SequenceLineNumberedReader reader = new SequenceLineNumberedReader();
        reader.initialize(split, context);
        return reader;
    }
}