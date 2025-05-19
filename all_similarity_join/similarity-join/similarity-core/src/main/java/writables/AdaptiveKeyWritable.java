package writables;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import utils.Hash;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class AdaptiveKeyWritable extends BinaryComparable implements WritableComparable<BinaryComparable> {

    private static final int BYTES_SIZE = 8;
    private static final int I_INDEX = 0;
    private static final int I_KEY = 1;
    private static final int I_REDUCER = 5;
    private static final int I_SHIFT = 7;

    private final byte[] bytes;
    private final ByteBuffer buff;

    public AdaptiveKeyWritable(){
        bytes = new byte[BYTES_SIZE];
        buff = ByteBuffer.wrap(bytes);
    }

    public long getKey() {
        return Hash.getHash(buff.get(I_INDEX), buff.getInt(I_KEY));
    }

    public int getReducerId() {
        return buff.getShort(I_REDUCER);
    }

    public boolean isDistributed(){
        return getReducerId() != Short.MIN_VALUE;
    }

    public boolean isPrimaryKey(){
        return buff.get(I_SHIFT) == 0;
    }

    @Override
    public int getLength() {
        return BYTES_SIZE;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.write(bytes);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        in.readFully(this.bytes, 0, BYTES_SIZE);
    }

    @Override
    public byte[] getBytes(){
        return this.bytes;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof AdaptiveKeyWritable && super.equals(o);
    }

    @Override
    public int hashCode() {
        return (int) (getKey() >> 8);
    }

    public void setAll(long hash, short reducerId, byte shift) {
        buff.rewind();

        buff.put((byte) Hash.getI(hash))
                .putInt(Hash.getValue(hash))
                .putShort(reducerId)
                .put(shift);
    }

    public void setAll(long hash) {
        buff.rewind();
        buff.put((byte) Hash.getI(hash))
                .putInt(Hash.getValue(hash))
                .putShort(Short.MIN_VALUE)
                .put(Byte.MIN_VALUE);
    }

    @Override
    public String toString() {
        return Arrays.toString(bytes);
    }

    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(AdaptiveKeyWritable.class);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return compareBytes(b1, s1, l1, b2, s2, l2);
        }
    }

    static {
        WritableComparator.define(AdaptiveKeyWritable.class, new Comparator());
    }
}
