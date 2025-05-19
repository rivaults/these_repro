package writables.histogram;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static utils.SetupTools.MAX_FREQUENCY;

public class HistogramWritable implements Writable {

    private int idReducer;
    private int buckets;

    public HistogramWritable() {}

    public HistogramWritable(HistogramWritable from) {
        buckets = from.buckets;
        if (buckets > 1)
            idReducer = from.idReducer;
    }

    public int getIdReducer() {
        return idReducer;
    }

    public void setIdReducer(int idReducer) {
        this.idReducer = idReducer;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeBoolean(buckets > 1);
        if (buckets > 1) {
            WritableUtils.writeVInt(out, buckets);
            WritableUtils.writeVInt(out, idReducer);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        boolean isHighlyFrequent = in.readBoolean();
        if (isHighlyFrequent) {
            buckets = WritableUtils.readVInt(in);
            idReducer = WritableUtils.readVInt(in);
        }else{
            buckets = 1;
        }
    }

    public void copy(HistogramWritable h) {
        buckets = h.buckets;
        idReducer = h.idReducer;
    }

    @Override
    public HistogramWritable clone() {
        return new HistogramWritable(this);
    }

    public void setBucketsNbByFrequency(int f) {
        int b = (int) Math.ceil(f /MAX_FREQUENCY);
        buckets =  (b % 2 == 1) ? b : (b + 1);
    }

    public int getNbBuckets() {
        return buckets;
    }

    @Override
    public String toString() {
        return "HistogramWritable{" +
                "idReducer=" + idReducer +
                ", freq=" + buckets +
                '}';
    }
}
