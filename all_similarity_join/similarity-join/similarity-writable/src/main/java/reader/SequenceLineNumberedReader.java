package reader;

import exceptions.NotInAlphabetException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import writables.SequenceWritable;

import java.io.IOException;

import static writables.configuration.SequenceConfigurable.MIN_SEQ;

public class SequenceLineNumberedReader extends RecordReader<NullWritable, SequenceWritable> {

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
        boolean found = false;
        while(!found && in.nextKeyValue()) {
            found = true;
            int separator = in.getCurrentValue().find("\t");
            outV.setId(Long.parseLong(Text.decode(in.getCurrentValue().getBytes(), separator + 1, in.getCurrentValue().getLength() - separator - 1)));
            if (separator == -1) {
                throw new NullPointerException("Tokenizer Error : no separator found");
            }
            if (separator < MIN_SEQ) {
                found = false;
                continue;
            }
            outV.getSequence().set(in.getCurrentValue().getBytes(), 0, separator);
            try {
                outV.translate();
            } catch (NotInAlphabetException e) {
                found = false;
            }
        }
        return found;
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