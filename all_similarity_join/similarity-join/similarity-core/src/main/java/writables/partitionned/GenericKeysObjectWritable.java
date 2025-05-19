package writables.partitionned;

import writables.IdentifiedWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

public class GenericKeysObjectWritable extends GenericKeysWritable {

    private IdentifiedWritable obj;

    public GenericKeysObjectWritable(Class<? extends IdentifiedWritable> t) {
        super();
        if (t == null)
            throw new IllegalArgumentException("null valueClass");
        try{
            obj = t.getDeclaredConstructor().newInstance();
        } catch (IllegalAccessException | InstantiationException e) {
            e.printStackTrace();
            throw new IllegalArgumentException("valueClass instantiation error");
        } catch (InvocationTargetException | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    public GenericKeysObjectWritable(GenericKeysObjectWritable from){
        super(from);
        obj = from.obj.copy();
    }

    public IdentifiedWritable get(){
        return this.obj;
    }

    public void set(IdentifiedWritable w) {
        this.obj = w;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        obj.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        obj.readFields(in);
    }

    public GenericKeysObjectWritable copy(){
        return new GenericKeysObjectWritable(this);
    }

}
