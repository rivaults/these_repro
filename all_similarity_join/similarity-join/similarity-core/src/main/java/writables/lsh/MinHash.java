package writables.lsh;

import org.apache.hadoop.conf.Configuration;
import utils.Hash;
import writables.IdentifiedWritable;
import writables.SetWritable;

import java.io.DataInput;
import java.io.IOException;

public class MinHash extends LSH{

    public final static int SIZE_RND = 2 * 256 * 4;
    private long[] randoms;

    public MinHash(){}

    public MinHash(int id, DataInput rnd, int k, int kInner) throws IOException {
        super(id, rnd, k, kInner);
        randoms = new long[SIZE_RND];
        for(int i = 0; i < SIZE_RND; ++i)
            randoms[i] = rnd.readLong();
    }

    @Override
    public void configure(Configuration c, boolean estimator){
        super.configure(c, estimator);
    }

    @Override
    public void digest(IdentifiedWritable o) {
        SetWritable s = (SetWritable) o;
        for(int j = 0; j < keysLength; ++j)
            keys[j] = Integer.MAX_VALUE;
        int unshift = (Integer.highestOneBit(keysLength) << 1)-1;
        for (int e : s.getElements()) {
            int h = Hash.digest(e, randoms, 0);
            int p = getPartition(h, unshift);
            if (h < keys[p % keysLength]) {
                keys[p % keysLength] = h;
            }
        }
        for(int i = 0; i < keysLength; ++i) {
            int e = 1;
            while (keys[i] == Integer.MAX_VALUE) {
                keys[i] = keys[(i + e++) % keysLength];
            }
        }
    }

    private static int getPartition(int h, int unshift){
        return (h >>> 8) & unshift;
    }

    @Override
    public long hash() {
        return Hash.digestLongs(seed, keys);
    }
}
