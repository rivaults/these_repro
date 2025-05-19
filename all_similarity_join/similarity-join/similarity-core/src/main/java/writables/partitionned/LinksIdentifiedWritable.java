package writables.partitionned;

import org.apache.hadoop.io.Writable;
import writables.IdentifiedWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static utils.SetupTools.FACTORY;

public class LinksIdentifiedWritable implements Writable {

    private IdentifiedWritable obj;
    private final List<Long> links;

    public LinksIdentifiedWritable() throws InstantiationException, IllegalAccessException {
        obj = FACTORY.getObjectInstance();
        links = new ArrayList<>();
    }

    public LinksIdentifiedWritable(LinksIdentifiedWritable f) {
        this.obj = f.obj.copy();
        this.links = new ArrayList<>(f.links);
    }

    public IdentifiedWritable getObj() {
        return obj;
    }

    public List<Long> getLinks() {
        return links;
    }

    public void setAll(IdentifiedWritable f, List<Long> l){
        obj = f;
        links.clear();
        if (l != null)
            links.addAll(l);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        obj.write(out);
        out.writeInt(links.size());
        for(long l : links)
            out.writeLong(l);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        links.clear();
        obj.readFields(in);
        int size = in.readInt();
        for(int i = 0; i < size; ++i)
            links.add(in.readLong());
    }

}
