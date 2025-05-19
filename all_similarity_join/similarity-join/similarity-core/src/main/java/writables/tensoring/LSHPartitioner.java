package writables.tensoring;

import org.apache.hadoop.conf.Configuration;
import utils.Hash;
import writables.IdentifiedWritable;
import writables.lsh.LSH;
import writables.lsh.SlicedQGramsLSH;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;

import static utils.SetupTools.NB_CONCATENATION;
import static utils.SetupTools.NB_INNER_CONCATENATION;

public class LSHPartitioner{

    private static final String LSH_CLS = "lsh.class";

    LSH[] partitioners;
    private long seed;

    public LSHPartitioner(){}

    public void initialize() {
        currentKeys = new long[NB_CONCATENATION * NB_INNER_CONCATENATION];
    }

    public void writeAll(Configuration c, DataInput rnd, DataOutput out) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException, IOException {
        int size = c.getInt("partitioner.size", 1);
        partitioners = new LSH[size];
        for (int i = 0; i < size; ++i) {
            partitioners[i] = c.getClass(LSH_CLS, SlicedQGramsLSH.class, LSH.class)
                    .getConstructor(Integer.TYPE, DataInput.class, Integer.TYPE, Integer.TYPE)
                    .newInstance(i, rnd, NB_CONCATENATION, NB_INNER_CONCATENATION);
            partitioners[i].configure(c, false);
            partitioners[i].write(out);
        }
        out.writeLong(seed);
    }

    public void readAll(Configuration c, DataInput in) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException, IOException {
        int size = c.getInt("partitioner.size", 1);
        partitioners = new LSH[size];
        for (int i = 0; i < size; ++i) {
            partitioners[i] = c.getClass(LSH_CLS, SlicedQGramsLSH.class, LSH.class).getDeclaredConstructor().newInstance();
            partitioners[i].readFields(in);
        }
        for (int i = 0; i < size; ++i) {
            partitioners[i].configure(c, false);
        }
        seed = in.readLong();
    }

    public void digest(IdentifiedWritable o){
        currentId = 0;
        for(LSH f: partitioners) {
            f.digest(o);
        }
    }

    private int currentId;
    private long[] currentKeys;

    public long next(){
        for(int k = 0; k < NB_CONCATENATION; ++k) {
            for(int kinner = 0; kinner < NB_INNER_CONCATENATION; ++kinner) {
                currentKeys[k* NB_INNER_CONCATENATION +kinner] = partitioners[0].getKey(
                        k,
                        kinner,
                        currentId
                );
            }
        }
        currentId += 1;
        return Hash.getHash(currentId, Hash.digestLongs(seed, currentKeys));
    }

    @Override
    public String toString() {
        return Arrays.toString(currentKeys);
    }
}
