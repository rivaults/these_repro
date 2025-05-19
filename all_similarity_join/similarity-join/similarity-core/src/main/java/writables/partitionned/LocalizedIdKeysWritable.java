package writables.partitionned;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LocalizedIdKeysWritable extends GenericKeysWritable {
    private LocalizedIdWritable localizedId;

    public LocalizedIdKeysWritable() {
        super();
        localizedId = new LocalizedIdWritable();
    }

    public LocalizedIdKeysWritable(int splitId, int localization, long id) {
        super();
        localizedId = new LocalizedIdWritable(splitId, localization, id);
    }

    public LocalizedIdKeysWritable(LocalizedIdKeysWritable from) {
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
    public LocalizedIdKeysWritable copy() {
        return new LocalizedIdKeysWritable(this);
    }
}
