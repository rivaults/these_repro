package writables.histogram;

import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class HistogramPartitionsWritable extends HistogramWritable {

    private final Set<Integer> partitions;

    public HistogramPartitionsWritable() {
        super();
        this.partitions = new HashSet<>();
    }

    public HistogramPartitionsWritable(HistogramPartitionsWritable from) {
        super(from);
        this.partitions = new HashSet<>(from.partitions);
    }

    public Set<Integer> getPartitions() {
        return partitions;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        WritableUtils.writeVInt(out, partitions.size());
        for (int p : partitions)
            WritableUtils.writeVInt(out, p);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        partitions.clear();
        int size = WritableUtils.readVInt(in);
        for(int i = 0; i < size; ++i)
            partitions.add(WritableUtils.readVInt(in));
    }

    @Override
    public String toString() {
        return super.toString() +
                " partitions=" + partitions;
    }

    @Override
    public HistogramPartitionsWritable clone() {
        return new HistogramPartitionsWritable(this);
    }
}
