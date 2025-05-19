package writables;

import com.google.common.base.Objects;
import exceptions.NotInAlphabetException;
import org.apache.hadoop.io.Text;
import writables.configuration.SequenceConfigurable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class SequenceWritable extends IdentifiedWritable {

    private final Text sequence;

    public SequenceWritable() {
        sequence = new Text();
    }

    public SequenceWritable(long id, Text e) {
        super(id);
        this.sequence = new Text(e.copyBytes());
    }

    public SequenceWritable(SequenceWritable from){
        super(from.getId());
        this.sequence = new Text(from.sequence.copyBytes());
    }

    public Text getSequence() {
        return sequence;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        sequence.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        sequence.readFields(in);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SequenceWritable that = (SequenceWritable) o;
        return super.getId() == that.getId() && Objects.equal(sequence, that.sequence);
    }

    @Override
    public IdentifiedWritable copy() {
        return new SequenceWritable(this);
    }

    @Override
    public byte[] getBytes() {
        return sequence.getBytes();
    }

    @Override
    public int getLength(){
        return sequence.getLength();
    }

    public byte getByte(int i) {
        return sequence.getBytes()[i];
    }

    public void translate() throws NotInAlphabetException {
        for (int i = 0; i < getLength(); ++i) {
            if (SequenceConfigurable.ENCODER.containsKey(getByte(i)))
                getBytes()[i] = SequenceConfigurable.ENCODER.get(getByte(i));
            else
                throw new NotInAlphabetException();
        }
    }

    static{
        conf = new SequenceConfigurable();
    }
}
