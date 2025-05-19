package writables.histogram;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LongHistGenericWritable implements Writable {

    long key;
    HistogramWritable histogram;

    public LongHistGenericWritable(Class<? extends HistogramWritable> t){
        if (t == null)
            throw new IllegalArgumentException("null valueClass");
        try{
            histogram = t.newInstance();
        } catch (IllegalAccessException | InstantiationException e) {
            e.printStackTrace();
            throw new IllegalArgumentException("null valueClass");
        }
    }

    public long getKey() {
        return key;
    }

    public void setKey(long k) {
        this.key = k;
    }

    public HistogramWritable getHistogram() {
        return histogram;
    }

    public void setHistogram(HistogramWritable histogram) {
        this.histogram.copy(histogram);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeVLong(out, key);
        this.histogram.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        key = WritableUtils.readVLong(in);
        this.histogram.readFields(in);
    }
}
