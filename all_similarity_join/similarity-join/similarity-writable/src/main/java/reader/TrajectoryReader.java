package reader;

import org.apache.hadoop.io.NullWritable;
import writables.TrajectoryWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static writables.configuration.TrajectoryConfigurable.DIMENSION;

public class TrajectoryReader extends RecordReader<NullWritable, TrajectoryWritable> {

    private LineRecordReader in;
    private static TrajectoryWritable v;
    private final String SEPARATOR = " ";
    private final Pattern PATTERN_POLYLINE = Pattern.compile("\\[(?:([+\\-]?\\d+(?:\\.\\d+)*),)*([+\\-]?\\d+(?:\\.\\d+)*)]");

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
        List<Double> pts = getPointsFromPolyLine(split[1]);
        v = new TrajectoryWritable(id, pts);
        if (pts.size() == 0)
            return nextKeyValue();
        return true;
    }

    private List<Double> getPointsFromPolyLine(String s) {
        List<Double> res = new ArrayList<>();
        Matcher matcher = PATTERN_POLYLINE.matcher(s);
        while (matcher.find()){
            for(int d = 0; d < DIMENSION; ++d)
                res.add(Double.parseDouble(matcher.group(1+d)));
        }
        return res;
    }

    @Override
    public NullWritable getCurrentKey() {
        return NullWritable.get();
    }

    @Override
    public TrajectoryWritable getCurrentValue(){
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