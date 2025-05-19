package reader;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import writables.SetWritable;

public class SetInput extends FileInputFormat<NullWritable, SetWritable> {

    @Override
    public RecordReader<NullWritable, SetWritable> createRecordReader(InputSplit split,
                                                                      TaskAttemptContext context)
            throws IOException {
        SetReader reader = new SetReader();
        reader.initialize(split, context);
        return reader;
    }
}