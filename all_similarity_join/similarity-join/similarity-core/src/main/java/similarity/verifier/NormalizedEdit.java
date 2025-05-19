package similarity.verifier;

import org.apache.hadoop.conf.Configuration;
import writables.SequenceWritable;

import static utils.SetupTools.THRESHOLD;

public class NormalizedEdit extends DistanceFields<SequenceWritable> {

    private final Edit proxy = new Edit();

    private double getThreshold(SequenceWritable u,
                                SequenceWritable v){
        return Math.max(u.getLength(), v.getLength()) * THRESHOLD;
    }

    @Override
    public boolean verify(SequenceWritable u, SequenceWritable v) {
        return proxy.verify(u, v, getThreshold(u, v));
    }

    @Override
    public boolean verify(SequenceWritable u, SequenceWritable v, double threshold) {
        return proxy.verify(u, v, threshold);
    }

    @Override
    public void configure(Configuration c) {}

    @Override
    public boolean sizeFilter(short ul, short vl) {
        return Math.max(ul, vl) * (1-THRESHOLD) < Math.min(ul, vl);
    }
}
