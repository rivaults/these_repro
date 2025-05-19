package experiments;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import utils.SetupTools;

import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.HashMap;

import static utils.SetupTools.*;

public class ExperimentReporter {
    public static final String DIR_EXP = "/exp/";

    private static void displayCounters(CounterGroup group, DataOutputStream out) throws IOException {
        for(Counter c : group)
            out.writeChars("\n" + c.getDisplayName() + " = " + c.getValue());
    }

    private static void displayConf(DataOutputStream out, Job job) throws IOException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
        SetupTools.setup(job.getConfiguration());
        out.writeChars("\nQ = " + NB_ITERATIONS);
        out.writeChars("\nK = " + NB_CONCATENATION);
    }

    public static void experiment(Job job, String p, boolean isJoinJob, long globalTime) throws IOException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
        FileSystem fs = FileSystem.get(URI.create(DIR_EXP), job.getConfiguration());
        Path path = new Path(DIR_EXP + p);
        fs.delete(path, true);
        DataOutputStream out = fs.create(path);
        displayCounters(job.getCounters().getGroup("org.apache.hadoop.mapreduce.TaskCounter"), out);
        displayCounters(job.getCounters().getGroup("org.apache.hadoop.mapreduce.FileSystemCounter"), out);
        displayCounters(job.getCounters().getGroup("org.apache.hadoop.mapreduce.JobCounter"), out);
        out.writeChars("\nTOTAL_TIME = " + globalTime);
        displayConf(out, job);
        if (isJoinJob) {
            displayCounters(job.getCounters().getGroup("experiments.RES_COUNTERS"), out);
        }
        displayCounters(job.getCounters().getGroup("experiments.TIME_COUNTERS"), out);
        out.close();
    }

    private final static HashMap<TIME_COUNTERS, Long> starts = new HashMap<>();

    public static void start(TIME_COUNTERS ...counters){
        long start = System.currentTimeMillis();
        for(TIME_COUNTERS c : counters)
            starts.put(c, start);
    }

    public static void end(TaskAttemptContext context, TIME_COUNTERS cTotal){
        ExperimentReporter.total(context, cTotal, System.currentTimeMillis() - starts.get(cTotal));
    }

    public static void total(TaskAttemptContext job, TIME_COUNTERS c, long v){
        Counter counter = job.getCounter(c);
        counter.increment(v);
    }
}
