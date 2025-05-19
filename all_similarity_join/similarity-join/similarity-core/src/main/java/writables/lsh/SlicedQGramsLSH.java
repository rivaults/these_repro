package writables.lsh;

import org.apache.hadoop.conf.Configuration;
import utils.Hash;
import writables.IdentifiedWritable;
import writables.SequenceWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;

import static writables.configuration.SequenceConfigurable.ALPHABET_SIZE;

public class SlicedQGramsLSH extends LSH {

    private int qSize = 4;
    private static final String S_SIZE_CONF = "qgram.length";


    public final static int SIZE_RND = 2 * 256 * 4 * 8;
    private final long[] randoms;
    private HashMap<Integer, Integer> multiplicity;

    @Override
    public void configure(Configuration c, boolean estimator){
        super.configure(c, estimator);
        qSize = c.getInt(
                (estimator)
                        ? SKETCHING_CONF + S_SIZE_CONF
                        : S_SIZE_CONF,
                qSize
        );
    }

    public SlicedQGramsLSH() {
        super();
        randoms = new long[SIZE_RND];
        multiplicity = new HashMap<>();
    }

    public SlicedQGramsLSH(int id, DataInput rnd, int k, int kInner) throws IOException {
        super(id, rnd, k, kInner);
        randoms = new long[SIZE_RND];
        for(int i = 0; i < SIZE_RND; ++i)
            randoms[i] = rnd.readLong();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        for(int i = 0; i < SIZE_RND; ++i)
            out.writeLong(randoms[i]);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        for(int i = 0; i < SIZE_RND; ++i)
            randoms[i] = in.readLong();
    }

    private int translate(SequenceWritable s, int start) {
        long res = 0L;
        int shift = 1+Integer.numberOfTrailingZeros(Integer.highestOneBit(ALPHABET_SIZE));
        for(int i = 0; i < qSize; ++i){
            res <<= shift;
            res += s.getByte(start+i);
        }
        return Long.hashCode(res);
    }

    private int getKPartition(int i, float l){
        int k = (int) (Math.max(i-1, 0) / Math.floor(l / keysLength));
        return Math.min(k, keysLength-1);
    }

    private int getPartition(int val){
        return (val & Integer.MAX_VALUE) % (partitionSize * keysInnerLength);
    }

    @Override
    public void digest(IdentifiedWritable o) {
        SequenceWritable s = (SequenceWritable) o;
        for (int i = 0; i < keysLength * partitionSize * keysInnerLength; ++i) {
            keys[i] = Long.MAX_VALUE;
        }
        for (int i = 0; i < s.getLength() - qSize; ++i) {
            int minK = translate(s, i);
            int minKP = getKPartition(i, s.getLength()- qSize +1);
            int minV = Hash.digest(minK, randoms, multiplicity.getOrDefault(minK, 0));
            int minP = getPartition(minV);
            int p = minKP * (partitionSize * keysInnerLength) + minP;
            if (minV < keys[p]) {
                keys[p] = minV;
            }
            multiplicity.put(minK, multiplicity.getOrDefault(minK, 0)+1);
        }
        for(int i = 0; i < keysLength; ++i) {
            for (int j = 0; j < partitionSize; ++j) {
                int e = 1;
                while (keys[i*partitionSize+j] == Long.MAX_VALUE) {
                    keys[i*partitionSize+j] = keys[i*partitionSize+((j+e++) % partitionSize)];
                }
            }
        }
        multiplicity.clear();
    }

    @Override
    public long hash(){
        return Hash.getHash(id, Hash.digestLongs(seed, keys));
    }

    public byte sketch(int k, int p) {
        byte res = 0;
        for(int i = partitionSize * k + p * Byte.SIZE; i < partitionSize * k + (p+1) * Byte.SIZE; ++i){
            res <<= 1;
            res += Hash.digestLong(keys[i]);
        }
        return res;
    }
}
