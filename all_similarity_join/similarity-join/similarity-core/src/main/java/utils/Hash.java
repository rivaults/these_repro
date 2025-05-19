package utils;

import java.util.List;
import java.util.Random;

public class Hash {

    private static final Random r = new Random();

    private final static int RND_BYTES = 256 * 8;
    private final static int RND_SECOND = 256 * 4 * 8;

    public static int digest(int x, long[] rnd, int m){
        long h = 0L;
        for (int i = 0; i < 4; ++i, x >>= 8) {
            h ^= rnd[RND_BYTES*i+Byte.toUnsignedInt((byte) x)+m];
        }
        long drv = h >> 32;
        for (int i = 0; i < 4; ++i, drv >>= 8) {
            h ^= rnd[RND_SECOND+RND_BYTES*i+Byte.toUnsignedInt((byte) drv)+m];
        }
        return (int)h;
    }

    public static int digestLongs(long seed, long... objs) {
        r.setSeed(seed);
        long sum = 0;
        for(long o : objs)
            sum += r.nextLong() * o;
        sum += r.nextLong();
        return (int) ((sum >>> 32) & Integer.MAX_VALUE);
    }

    public static int digestInts(long seed, List<Integer> objs) {
        r.setSeed(seed);
        long sum = 0;
        for(int o : objs)
            sum += r.nextLong() * o;
        sum += r.nextLong();
        return (int) ((sum >>> 32) & Integer.MAX_VALUE);
    }

    public static byte digestLong(long key) {
        return (byte) ((key >> 16) & 1);
    }

    public static int getI(long h){
        return (int) (h >>> 32);
    }

    public static int getValue(long h){
        return (int) ((h << 32) >>> 32);
    }

    public static long getHash(long i, int h){
        return (i << 32) + h;
    }

    public static int digestSplitId(int id, int dh){
        return (id << 16) + dh;
    }

    public static int getSplitId(int p) {
        return p >> 16;
    }

    public static int getHistId(int p){
        return (p << 16) >> 16;
    }
}
