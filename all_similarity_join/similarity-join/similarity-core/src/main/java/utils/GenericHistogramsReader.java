package utils;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import writables.histogram.HistogramWritable;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

import static utils.SetupTools.*;

public class GenericHistogramsReader<T extends HistogramWritable> {

    private final LongWritable kBuff = new LongWritable();

    private static SequenceFile.Reader reader = null;
    private int currentSplitId = -1;

    private static SequenceFile.Reader getReader(JobContext context, int id) throws IOException {
        assert context != null;
        URI[] cached = context.getCacheFiles();
        if (cached.length < 2)
            throw new IOException("No cached file found.");
        return new SequenceFile.Reader(
                context.getConfiguration(),
                SequenceFile.Reader.file(
                        new Path(cached[1+id])
                )
        );
    }

    public void getAllHist(TaskAttemptContext context, Map<Long, T> histograms, int id) throws IOException {
        histograms.clear();
        if (reader == null || currentSplitId != id) {
            reader = getReader(context, id);
            currentSplitId = id;
        }
        HistogramWritable vBuff = new HistogramWritable();
        while(reader.next(kBuff, vBuff)) {
            if (kBuff.get() == Long.MIN_VALUE) {
                groupMemory += 1;
                if (NB_ITERATIONS *SIZE_GROUP*0.66f < histograms.size())
                    break;
            }
            else if (!histograms.containsKey(kBuff.get()))
                histograms.put(kBuff.get(), (T) vBuff.clone());
        }
    }

    private int groupMemory;
    private int groupReader;
    private int counter;

    public void initialize(TaskAttemptContext context, Map<Long, T> histograms) throws IOException {
        counter = 0;
        groupMemory = 0;
        groupReader = 0;
        this.getAllHist(context, histograms, SetupTools.getIdSplit(context.getTaskAttemptID()));
    }

    public void update(TaskAttemptContext context, Map<Long, T> histograms) throws IOException {
        counter++;
        if (counter > SIZE_GROUP) {
            counter = 0;
            groupReader += 1;
            if (groupMemory == groupReader)
                this.getAllHist(context, histograms, SetupTools.getIdSplit(context.getTaskAttemptID()));
        }
    }
}
