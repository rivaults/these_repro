package writables.estimator;

import org.apache.hadoop.conf.Configuration;
import writables.IdentifiedWritable;

import java.io.DataInputStream;
import java.io.IOException;

public class NoSketch extends Sketching {

    public NoSketch() {}

    public NoSketch(DataInputStream f) throws IOException {
    }

    @Override
    public byte[] digest(IdentifiedWritable o) {
        return new byte[0];
    }

    @Override
    public void configure(Configuration c) {

    }

    @Override
    public boolean compareSketches(byte[] x, byte[] y) {
        return true;
    }
}
