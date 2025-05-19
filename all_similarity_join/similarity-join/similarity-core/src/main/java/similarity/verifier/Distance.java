package similarity.verifier;

import org.apache.hadoop.conf.Configuration;
import writables.IdentifiedWritable;
import writables.SequenceWritable;

public interface Distance<T extends IdentifiedWritable> {
    boolean verify(T u, T v);
    boolean verify(T u, T v, double threshold);
    void configure(Configuration c);
    boolean sizeFilter(short length, short length1);
}
