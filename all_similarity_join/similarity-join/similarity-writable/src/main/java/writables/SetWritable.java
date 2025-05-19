package writables;

import writables.configuration.SetConfigurable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class SetWritable extends IdentifiedWritable {

    private final ArrayList<Integer> elements;

    public SetWritable() {
        elements = new ArrayList<>();
    }

    public SetWritable(long id, List<Integer> from) {
        super(id);
        this.elements = new ArrayList<>(from);
    }

    public SetWritable(SetWritable from) {
        super(from.getId());
        this.elements = new ArrayList<>(from.elements);
    }

    public ArrayList<Integer> getElements() {
        return elements;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeInt(elements.size());
        for (int e : elements)
            out.writeInt(e);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        elements.clear();
        int size = in.readInt();
        for (int i = 0; i < size; ++i) {
            elements.add(in.readInt());
        }
    }

    @Override
    public IdentifiedWritable copy() {
        return new SetWritable(this);
    }

    @Override
    public byte[] getBytes() {
        ByteBuffer buff = ByteBuffer.allocate(elements.size() * 4);
        for(int e : elements)
            buff.putInt(e);
        return buff.array();
    }

    @Override
    public int getLength() {
        return getBytes().length;
    }

    static{
        conf = new SetConfigurable();
    }
}
