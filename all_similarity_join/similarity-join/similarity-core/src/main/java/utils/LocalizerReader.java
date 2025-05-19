package utils;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import writables.partitionned.LongListWritable;

import java.io.IOException;
import java.net.URI;
import java.util.List;

public class LocalizerReader {

    private static SequenceFile.Reader reader = null;
    private final IntWritable kBuff = new IntWritable();
    private final LongListWritable vBuff = new LongListWritable();

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

    private final Localizer localizer = new Localizer();
    private int nextStop = 0;

    private void update(List<Long> out) throws IOException {
        out.clear();
        boolean hasNext = reader.next(kBuff, vBuff);
        if (hasNext){
            nextStop = kBuff.get();
            out.addAll(vBuff.get());
        }
        vBuff.clear();
    }

    public void initialize(TaskAttemptContext context, List<Long> out) throws IOException {
        out.clear();
        int id = SetupTools.getIdSplit(context.getTaskAttemptID());
        if (reader == null || localizer.getSplitId() != id) {
            reader = getReader(context, id);
            localizer.reset(id);
        }
        update(out);
    }

    public boolean next(List<Long> out) throws IOException {
        localizer.next();
        if (nextStop < localizer.getLocalization())
            update(out);
        return localizer.getLocalization() == nextStop;
    }
}
