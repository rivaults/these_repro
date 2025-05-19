package writables.partitionned;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import static utils.SetupTools.*;
import static writables.estimator.Sketching.*;

public class SketchedWritable implements Writable {

    private final byte[] estimators;
    private short length;

    public SketchedWritable() {
        estimators = new byte[SKETCHES_BYTES];
    }

    public SketchedWritable(SketchedWritable from){
        estimators = Arrays.copyOf(from.estimators, from.estimators.length);
        length = from.length;
    }

    public void setEstimators(byte[] from){
        System.arraycopy(from, 0, estimators, 0, from.length);
    }

    public byte[] getEstimators() {
        return estimators;
    }

    public void setLength(short length) {
        this.length = length;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.write(estimators);
        out.writeShort(length);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        in.readFully(estimators);
        length = in.readShort();
    }

    public SketchedWritable copy(){
        return new SketchedWritable(this);
    }

    public boolean sizeFilter(SketchedWritable o){
        return DISTANCE.sizeFilter(length, o.length);
    }
}
