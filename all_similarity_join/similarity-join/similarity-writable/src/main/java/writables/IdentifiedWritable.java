package writables;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import utils.Configurable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public abstract class IdentifiedWritable implements Writable {

    private long id;
    protected static Configurable conf;

    public IdentifiedWritable() {}

    public IdentifiedWritable(long id) {
        this.id = id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getId() {
        return id;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(id);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id = in.readLong();
    }

    public abstract IdentifiedWritable copy();

    public abstract byte[] getBytes();

    public abstract int getLength();

    public static void setup(Configuration c){
        conf.setup(c);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IdentifiedWritable that = (IdentifiedWritable) o;
        return id == that.id;
    }

    @Override
    public int hashCode() {
        return (int) (id ^ (id >>> 32));
    }
}
