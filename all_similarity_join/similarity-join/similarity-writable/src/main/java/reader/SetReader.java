package reader;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import writables.SetWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

public class SetReader extends RecordReader<NullWritable, SetWritable> {

    private LineRecordReader in;
    private static SetWritable v;
    private final String SEPARATOR = " ";

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
        in = new LineRecordReader();
        in.initialize(split, context);
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        if (!in.nextKeyValue())
            return false;
        String line = in.getCurrentValue().toString();
        String[] split = line.split(SEPARATOR);
        long id = Long.parseLong(split[0]);
        ArrayList<Integer> elements = getElements(split);
        if (elements.size() == 0)
            return nextKeyValue();
        v = new SetWritable(id, elements);
        return true;
    }

    private ArrayList<Integer> getElements(String[] s) {
        ArrayList<Integer> res = new ArrayList<>();
        for(int i = 1; i < s.length; ++i)
            res.add(Integer.parseInt(s[i]));
        Collections.sort(res);
        return res;
    }

    @Override
    public NullWritable getCurrentKey() {
        return NullWritable.get();
    }

    @Override
    public SetWritable getCurrentValue(){
        return v;
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