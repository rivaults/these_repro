package writables.estimator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import writables.IdentifiedWritable;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;

import static utils.SetupTools.*;
import static writables.lsh.LSH.P_INNER_CONF;
import static writables.lsh.LSH.SKETCHING_CONF;

public abstract class Sketching implements Writable {

    protected static int SKETCHES_BITS = 192;
    protected static int PARTITION_BITS = 32;
    protected static int ESTIMATOR_SIZE = 3;
    public static int SKETCHES_BYTES = 32;
    public static int PARTITION_BYTES = 4;
    public static double SKETCHES_P1 = 0.14;

    public static void setup(Configuration c){
        SKETCHES_BITS = c.getInt("sketching.size", SKETCHES_BITS);
        SKETCHES_BYTES = SKETCHES_BITS / Byte.SIZE;
        PARTITION_BITS = c.getInt(SKETCHING_CONF + P_INNER_CONF, PARTITION_BITS);
        ESTIMATOR_SIZE = SKETCHES_BITS / (PARTITION_BITS * NB_CONCATENATION);
        PARTITION_BYTES = PARTITION_BITS / Byte.SIZE;
        SKETCHES_P1 = c.getDouble("sketching.p1", 0.25);
    }

    long[] seeds;
    long seed;

    public Sketching(){
        seeds = new long[ESTIMATOR_SIZE];
    }

    public Sketching(DataInputStream rnd) throws IOException {
        seed = rnd.readLong();
        seeds = new long[ESTIMATOR_SIZE];
        for(int i = 0; i < ESTIMATOR_SIZE; ++i){
            seeds[i] = rnd.readLong();
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(seed);
        for(int i = 0; i < ESTIMATOR_SIZE; ++i)
            out.writeLong(seeds[i]);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        seed = in.readLong();
        for(int i = 0; i < ESTIMATOR_SIZE; ++i)
            seeds[i] = in.readLong();
    }

    public abstract byte[] digest(IdentifiedWritable o);

    public abstract void configure(Configuration c);

    public abstract boolean compareSketches(byte[] x, byte[] y);
}
