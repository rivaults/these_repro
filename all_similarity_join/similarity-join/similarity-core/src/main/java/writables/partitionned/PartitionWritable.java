package writables.partitionned;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PartitionWritable implements Writable {

    private long key;

    public PartitionWritable(long key) {
        this.key = key;
    }

    public PartitionWritable() {}

    public PartitionWritable(PartitionWritable from) {
        key = from.key;
    }

    public void set(long k, boolean b){
        this.key = k;
    }

    public long getKey() {
        return key;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(key);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        key = in.readLong();
    }
}
