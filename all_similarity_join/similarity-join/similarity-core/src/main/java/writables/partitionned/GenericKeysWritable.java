package writables.partitionned;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import static utils.SetupTools.NB_ITERATIONS;

public class GenericKeysWritable extends SketchedWritable {

    private final Long[] keys;

    public GenericKeysWritable() {
        keys = new Long[NB_ITERATIONS];
    }

    public GenericKeysWritable(GenericKeysWritable from){
        keys = Arrays.copyOf(from.keys, from.keys.length);
    }

    public Long[] getKeys() {
        return keys;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        for(int i = 0; i < NB_ITERATIONS; i+=8){
            byte b = 0;
            for(int j = 0; j < 8; ++j){
                b <<= 1;
                b += (byte) ((i+j < NB_ITERATIONS && keys[i+j] != null) ? 1 : 0);
            }
            out.writeByte(b);
            for(int j = 0; j < 8; ++j){
                if (i+j < NB_ITERATIONS && keys[i+j] != null)
                    out.writeLong(keys[i+j]);
            }
        }
    }

    private boolean getBit(byte b, int position)
    {
        return (((b >> 7-position) & 1) == 1);
    }


    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        for (int i = 0; i < NB_ITERATIONS; i+=8) {
            byte b = in.readByte();
            for(int j = 0; j < 8; ++j) {
                if (getBit(b, j))
                    keys[i+j] = in.readLong();
                else if (i+j < NB_ITERATIONS)
                    keys[i+j] = null;
            }
        }
    }

    public Long getKey(int i) {
        return keys[i];
    }

    public void setKey(int i, Long o) {
        keys[i] = o;
    }

    public GenericKeysWritable copy(){
        return new GenericKeysWritable(this);
    }
}
