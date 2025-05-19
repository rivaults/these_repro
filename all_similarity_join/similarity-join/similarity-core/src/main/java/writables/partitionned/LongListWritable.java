package writables.partitionned;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class LongListWritable implements Writable {

    private final ArrayList<Long> transfers;

    public LongListWritable() {
        transfers = new ArrayList<>();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(transfers.size());
        for(long e : transfers)
            out.writeLong(e);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        transfers.clear();
        int size = in.readInt();
        for(int i = 0; i < size; ++i)
            transfers.add(in.readLong());
    }

    public void addAll(Collection<Long> from){
        transfers.clear();
        transfers.addAll(from);
    }

    public void clear() {
        transfers.clear();
    }

    public List<Long> get(){
        return transfers;
    }
}
