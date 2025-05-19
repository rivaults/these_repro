package writables.estimator;

import org.apache.hadoop.conf.Configuration;
import writables.IdentifiedWritable;
import writables.lsh.SlicedQGramsLSH;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;

import static utils.SetupTools.*;

public class EditSketching extends Sketching {

    private static int MINIMUM_SIZE_ESTIMATED = 0;
    private final SlicedQGramsLSH[] estimators;
    private byte[] sketches;

    public EditSketching() {
        estimators = new SlicedQGramsLSH[ESTIMATOR_SIZE];
        sketches = new byte[SKETCHES_BYTES];
    }

    public EditSketching(DataInputStream rnd) throws IOException {
        super(rnd);
        estimators = new SlicedQGramsLSH[ESTIMATOR_SIZE];
        for(int i = 0; i < ESTIMATOR_SIZE; ++i){
            estimators[i] = new SlicedQGramsLSH(i, rnd, NB_CONCATENATION, NB_INNER_CONCATENATION);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        for(int i = 0; i < ESTIMATOR_SIZE; ++i)
            estimators[i].write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        for(int i = 0; i < ESTIMATOR_SIZE; ++i) {
            estimators[i] = new SlicedQGramsLSH();
            estimators[i].readFields(in);
        }
    }

    @Override
    public byte[] digest(IdentifiedWritable o) {
        if (o.getLength() < MINIMUM_SIZE_ESTIMATED)
            return new byte[0];
        for(int i = 0; i < ESTIMATOR_SIZE; ++i) {
            estimators[i].digest(o);
        }
        for(int i = 0; i < ESTIMATOR_SIZE; ++i) {
            for(int k = 0; k < NB_CONCATENATION; ++k){
                for(int p = 0; p < PARTITION_BYTES; ++p)
                    sketches[i * NB_CONCATENATION * PARTITION_BYTES + k * PARTITION_BYTES + p] = estimators[i].sketch(k, p);
            }
        }
        return sketches;
    }

    public void configure(Configuration c){
        MINIMUM_SIZE_ESTIMATED = c.getInt("lsh.estimator.min.size", MINIMUM_SIZE_ESTIMATED);
        for (int i = 0; i < ESTIMATOR_SIZE; ++i) {
            estimators[i].configure(c, true);
        }
    }

    public boolean compareSketches(byte[] from, byte[] to){
        int res = 0;
        for (int i = 0; i < from.length; ++i) {
            int a = Byte.toUnsignedInt(from[i]);
            int b = Byte.toUnsignedInt(to[i]);
            res += Integer.bitCount(a ^ b);
        }
        float score = 1f - (res / (from.length * Byte.SIZE * 1f));
        return (1+SKETCHES_P1)/2f <= score;
    }
}
