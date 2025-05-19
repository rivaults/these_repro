package reader;

import exceptions.NotInAlphabetException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import writables.SequenceWritable;

import java.io.IOException;

public class FASTAReader extends RecordReader<NullWritable, SequenceWritable> {

    private LineRecordReader in;
    private SequenceWritable outV;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
        in = new LineRecordReader();
        in.initialize(split, context);
        outV = new SequenceWritable();
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        if (!in.nextKeyValue())
            return false;
        outV.setId(in.getCurrentKey().get());
        outV.getSequence().clear();
        while (in.nextKeyValue()) {
            if (in.getCurrentValue().getLength() == 0)
                break;
            if (in.getCurrentValue().find(">") != 0) {
                outV.getSequence().append(in.getCurrentValue().getBytes(), 0, in.getCurrentValue().getLength());
            }
        }
        if (outV.getLength() == 0)
            return nextKeyValue();
        try {
            outV.translate();
        }
        catch (NotInAlphabetException e){
            return nextKeyValue();
        }
        return true;
    }

    @Override
    public NullWritable getCurrentKey() {
        return NullWritable.get();
    }

    @Override
    public SequenceWritable getCurrentValue(){
        return outV;
    }

    @Override
    public float getProgress() throws IOException {
        return in.getProgress();
    }

    @Override
    public void close() throws IOException {
        in.close();
    }
}