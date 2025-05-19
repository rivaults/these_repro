package writables.partitionned;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class LocalizedIdSketchedWritable extends SketchedWritable {

    private LocalizedIdWritable localizedId;

    public LocalizedIdSketchedWritable() {
        super();
        localizedId = new LocalizedIdWritable();
    }

    public LocalizedIdSketchedWritable(int splitId, int localization, long id) {
        super();
        localizedId = new LocalizedIdWritable(splitId, localization, id);
    }

    public LocalizedIdSketchedWritable(LocalizedIdSketchedWritable from) {
        super(from);
        this.localizedId = new LocalizedIdWritable(from.localizedId);
    }

    public LocalizedIdWritable getLocalizedId() {
        return localizedId;
    }

    public void setLocalizedId(LocalizedIdWritable localizedId) {
        this.localizedId = localizedId;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        localizedId.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        localizedId.readFields(in);
    }

    @Override
    public LocalizedIdSketchedWritable copy() {
        return new LocalizedIdSketchedWritable(this);
    }

    @Override
    public String toString() {
        return Arrays.toString(getEstimators());
    }
}
