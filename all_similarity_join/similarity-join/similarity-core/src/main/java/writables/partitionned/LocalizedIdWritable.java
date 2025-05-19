package writables.partitionned;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LocalizedIdWritable implements Writable {

    private int splitId;
    private int localization;
    private long id;

    public LocalizedIdWritable() {}

    public LocalizedIdWritable(int splitId, int localization, long id) {
        this.splitId = splitId;
        this.localization = localization;
        this.id = id;
    }

    public LocalizedIdWritable(LocalizedIdWritable from) {
        this.splitId = from.splitId;
        this.localization = from.localization;
        this.id = from.id;
    }

    public int getLocalization() {
        return localization;
    }

    public void setLocalization(int localization) {
        this.localization = localization;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public int getSplitId() {
        return splitId;
    }

    public void setSplitId(int splitId) {
        this.splitId = splitId;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeVInt(out, splitId);
        WritableUtils.writeVInt(out, localization);
        WritableUtils.writeVLong(out, id);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        splitId = WritableUtils.readVInt(in);
        localization = WritableUtils.readVInt(in);
        id = WritableUtils.readVLong(in);
    }
}
