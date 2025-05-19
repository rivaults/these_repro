package writables.histogram;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

public class IntPairWritable extends BinaryComparable implements WritableComparable<BinaryComparable> {

    private final int SIZE_KEY = 8;
    int splitId;
    int histId;
    byte[] bytes;

    public IntPairWritable() {
        this.bytes = ByteBuffer.allocate(SIZE_KEY).array();
    }

    @Override
    public int getLength() {
        return SIZE_KEY;
    }

    @Override
    public byte[] getBytes() {
        return bytes;
    }

    public int getSplitId() {
        return splitId;
    }

    public int getHistId() {
        return histId;
    }

    public void setSplitId(int splitId) {
        this.splitId = splitId;
        ByteBuffer.wrap(bytes).putInt(splitId).putInt(histId);
    }

    public void setHistId(int histId) {
        this.histId = histId;
        ByteBuffer.wrap(bytes).putInt(splitId).putInt(histId);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.write(bytes);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        splitId = in.readInt();
        histId = in.readInt();
        ByteBuffer.wrap(bytes).putInt(splitId).putInt(histId);
    }

    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(IntPairWritable.class);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return compareBytes(b1, s1, l1, b2, s2, l2);
        }
    }

    static {
        WritableComparator.define(IntPairWritable.class, new IntPairWritable.Comparator());
    }
}
