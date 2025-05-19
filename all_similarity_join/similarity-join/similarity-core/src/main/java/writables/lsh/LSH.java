package writables.lsh;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import writables.IdentifiedWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Random;

public abstract class LSH implements Writable {

    int partitionSize = 1;
    int keysLength;
    int keysInnerLength;
    int id;
    final Random rnd = new Random();
    long seed;
    long[] keys;

    public static final String SKETCHING_CONF = "sketching.";
    public static final String P_INNER_CONF = "P.inner";

    public LSH() {}

    public LSH(int id, DataInput rnd, int k, int kInner) throws IOException {
        this.id = id;
        this.seed = rnd.readLong();
        this.keysLength = k;
        this.keysInnerLength = kInner;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public Random getRnd() {
        return rnd;
    }

    public long getSeed() {
        return seed;
    }

    public void setSeed(long seed) {
        this.seed = seed;
    }

    public long[] getKeys() {
        return keys;
    }

    public long getKey(int k, int kInner, int p){
        return keys[partitionSize * k + keysInnerLength * p + kInner];
    }

    public int getPartitionSize() {
        return partitionSize;
    }

    public void setKeys(long[] keys) {
        this.keys = keys;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(id);
        out.writeLong(seed);
        out.writeInt(keysLength);
        out.writeInt(keysInnerLength);
        out.writeInt(partitionSize);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id = in.readInt();
        seed = in.readLong();
        keysLength = in.readInt();
        keysInnerLength = in.readInt();
        partitionSize = in.readInt();
    }

    public abstract void digest(IdentifiedWritable o);

    public abstract long hash();

    public void configure(Configuration c, boolean estimator){
        partitionSize = c.getInt(
                (estimator)
                        ? SKETCHING_CONF + P_INNER_CONF
                        : P_INNER_CONF,
                partitionSize
        );
        keys = new long[partitionSize * keysLength * keysInnerLength];
        rnd.setSeed(seed);
    }

    @Override
    public String toString() {
        return "LSH{" +
                "keysLength=" + keysLength +
                ", id=" + id +
                '}';
    }
}
