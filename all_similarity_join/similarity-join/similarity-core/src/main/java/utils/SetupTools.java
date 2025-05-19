package utils;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import writables.IdentifiedWritable;
import writables.SequenceWritable;
import experiments.ExperimentReporter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import similarity.verifier.Distance;
import similarity.verifier.Edit;
import writables.estimator.EditSketching;
import writables.estimator.NoSketch;
import writables.estimator.Sketching;
import writables.partitionned.GenericKeysObjectWritable;
import writables.partitionned.SequenceKeysWritable;
import writables.tensoring.LSHPartitioner;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.Arrays;

/**
 * Static properties setup using the XML configuration.
 */
public class SetupTools {

    /**
     * Distance threshold
     */
    public static double THRESHOLD = 0.15;

    /**
     * LSH probability of collision for similar object
     */
    public static double P1 = THRESHOLD;

    /**
     * LSH probability of collision for dissimilar object
     */
    public static double P2 = THRESHOLD;

    /**
     * Approximation Factor (when the LSH family allows it)
     */
    public static double C = 0.5;

    /**
     * F_Max : Maximum frequency for buckets during the randomized templates generation
     */
    public static double MAX_FREQUENCY = 1000d;

    /**
     * Size of a group for chunk partitioning,
     * i.e., splits are divided into group that allows the corresponding distributed histogram to fit in memory
     */
    public static int SIZE_GROUP = 10000;

    /**
     * Get full report of a hadoop execution
     */
    public static boolean COUNTERS_ENABLED = true;

    /**
     * False for local experiments
     */
    public static boolean EXPERIMENTS_ENABLED = false;

    /**
     * Number of iterations (L or Q)
     */
    public static int NB_ITERATIONS = 8;

    /**
     * Number of concatenations (K)
     */
    public static int NB_CONCATENATION = 1;

    /**
     * Number of inner concatenations : 1
     */
    public static int NB_INNER_CONCATENATION = 1;

    /**
     * rho = log(p1)/log(p2) (when the LSH family allows it)
     */
    public static double RHO;

    /**
     * Mean of collision for similar object
     */
    public static float LSH_MEAN = 2f;

    /**
     * Number of processor
     */
    public static int NB_PROCESSOR = 24;

    public static Distance DISTANCE;
    public static Sketching SKETCH;

    /**
     * Writables Factory
     */
    public static Factory FACTORY;

    public static double log(double base, double logNumber) {
        return Math.log(logNumber) / Math.log(base);
    }

    public static void setup(Configuration c) throws InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        THRESHOLD = c.getDouble("distance.threshold", THRESHOLD);
        MAX_FREQUENCY = c.getDouble("key.buckets.size", MAX_FREQUENCY);
        COUNTERS_ENABLED = c.getBoolean("debug.counters.enabled", COUNTERS_ENABLED);
        EXPERIMENTS_ENABLED = c.getBoolean("experiments.enabled", EXPERIMENTS_ENABLED);
        SIZE_GROUP = c.getInt("hist.max.inmem", SIZE_GROUP);
        /* Optimal */
        P1 = c.getDouble("lsh.p1", THRESHOLD);
        C = c.getDouble("lsh.c", C);
        P2 = c.getDouble("lsh.p2", C * P1);
        RHO = Math.log(P1)/Math.log(P2);
        LSH_MEAN = c.getFloat("lsh.mean", LSH_MEAN);
        NB_PROCESSOR = c.getInt("processor", NB_PROCESSOR);
        double p1 = 1f/(Math.pow(NB_PROCESSOR, (RHO /(1f+RHO))));
        NB_CONCATENATION = c.getInt("lsh.K", (int)Math.ceil(log(P1, p1)));
        NB_INNER_CONCATENATION = c.getInt("lsh.K.inner", 1);
        NB_ITERATIONS = c.getInt("lsh.L", (int)Math.ceil(LSH_MEAN *Math.pow(P1, -NB_CONCATENATION)));
        FACTORY = new Factory(
                c.getClass("object.class", SequenceWritable.class, IdentifiedWritable.class),
                c.getClass("object.keys.class", SequenceKeysWritable.class, GenericKeysObjectWritable.class)
        );
        FACTORY.getObjectInstance().setup(c);
        DISTANCE = c.getClass("distance.class", Edit.class, Distance.class).getDeclaredConstructor().newInstance();
        DISTANCE.configure(c);
        SKETCH = c.getClass(LSH_ESTIMATOR_CLS, NoSketch.class, Sketching.class).getDeclaredConstructor().newInstance();

    }

    public static void setCompress(Job job) {
        if (job.getConfiguration().getBoolean("compress", false)) {
            job.getConfiguration().set("mapreduce.map.output.compress", "true");
            job.getConfiguration().set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
            SequenceFileOutputFormat.setCompressOutput(job, true);
            SequenceFileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
            SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
        }
    }

    public static void setCompressOutGzip(Job job) {
        if (job.getConfiguration().getBoolean("compress", false)) {
            job.getConfiguration().set("mapreduce.map.output.compress", "true");
            job.getConfiguration().set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
            SequenceFileOutputFormat.setCompressOutput(job, true);
            SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
            SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
        }
    }

    public static void setCompressAllGzip(Job job) {
        if (job.getConfiguration().getBoolean("compress", false)) {
            job.getConfiguration().set("mapreduce.map.output.compress", "true");
            job.getConfiguration().set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.GzipCodec");
            SequenceFileOutputFormat.setCompressOutput(job, true);
            SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
            SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
        }
    }

    public static void setNbReducers(Job job, String reducers) {
        job.setNumReduceTasks(Integer.parseInt(reducers));
    }

    public static int getIdSplit(TaskAttemptID context){
        return context.getTaskID().getId();
    }

    public static void addCacheFiles(Job job, String dir) throws IOException {
        FileSystem fs = FileSystem.get(job.getConfiguration());
        FileStatus[] files = fs.listStatus(new Path(dir));
        Arrays.sort(files);
        for (FileStatus file : files) {
            job.addCacheFile(file.getPath().toUri());
        }
    }

    public static void run(Job job, String pathExpSample) throws IOException, InterruptedException, ClassNotFoundException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
        long start = System.currentTimeMillis();
        if (job.waitForCompletion(true)){
            long end = System.currentTimeMillis();
            if (EXPERIMENTS_ENABLED)
                ExperimentReporter.experiment(job, pathExpSample, true, end - start);
            System.exit(0);
        }
        System.exit(1);
    }

    private static final String LSH_ESTIMATOR_CLS = "lsh.estimator.class";

    public static LSHPartitioner readPartitionners(JobContext context, boolean readEstimator) throws IOException, InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        assert context != null;
        URI[] cached = context.getCacheFiles();
        if (cached.length < 1)
            throw new IOException("No cached file found.");
        Path path = new Path(cached[0]);
        FileSystem fs =  FileSystem.get(context.getConfiguration());
        FSDataInputStream reader = fs.open(path);
        LSHPartitioner p = new LSHPartitioner();
        p.initialize();
        p.readAll(context.getConfiguration(), reader);
        if (readEstimator){
            Sketching.setup(context.getConfiguration());
            SKETCH = context.getConfiguration().getClass(LSH_ESTIMATOR_CLS, EditSketching.class, Sketching.class).getDeclaredConstructor().newInstance();
            SKETCH.readFields(reader);
            SKETCH.configure(context.getConfiguration());
        }
        reader.close();
        return p;
    }

    public static final Path PATH_SEEDS = new Path("seeds/lsh");
    public static final Path PATH_PARTITIONERS = new Path("randoms/1-partitioners");

    public static void writePartitionners(Job job) throws IOException, InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        FileSystem fs = FileSystem.get(job.getConfiguration());
        FSDataInputStream f = fs.open(PATH_SEEDS, 1024);
        fs.delete(PATH_PARTITIONERS, true);
        DataOutputStream out = fs.create(PATH_PARTITIONERS);
        LSHPartitioner p = new LSHPartitioner();
        p.initialize();
        p.writeAll(job.getConfiguration(), f, out);
        Sketching.setup(job.getConfiguration());
        Sketching estimator = job.getConfiguration()
                .getClass(LSH_ESTIMATOR_CLS, EditSketching.class, Sketching.class)
                .getConstructor(DataInputStream.class)
                .newInstance(f);
        estimator.write(out);
        job.addCacheFile(PATH_PARTITIONERS.toUri());
        out.close();
        f.close();
    }
}