package reader;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import writables.SequenceWritable;

import java.io.IOException;

public class SequenceLineInput extends FileInputFormat<NullWritable, SequenceWritable> {

    @Override
    public RecordReader<NullWritable, SequenceWritable> createRecordReader(InputSplit split,
                                                                      TaskAttemptContext context)
            throws IOException {
        SequenceLineReader reader = new SequenceLineReader();
        reader.initialize(split, context);
        return reader;
    }
}