package similarity.verifier;

import org.apache.hadoop.conf.Configuration;
import writables.SetWritable;

import static utils.SetupTools.THRESHOLD;

public class Jaccard extends DistanceFields<SetWritable> {

    @Override
    public void configure(Configuration c) {}

    @Override
    public boolean sizeFilter(short ul, short vl) {
        return true;
    }

    public boolean verify(SetWritable u, SetWritable v) {
        return verify(u, v, THRESHOLD);
    }

    @Override
    public boolean verify(SetWritable u, SetWritable v, double threshold) {
        int olap = 0;
        int required = (int) Math.ceil(threshold / (1 + threshold) * (u.getElements().size() + v.getElements().size()));
        int pos1 = 0;
        int pos2 = 0;
        int max1 = u.getElements().size();
        int max2 = v.getElements().size();
        while (olap < required && max1 >= required && max2 >= required) {
            if (u.getElements().get(pos1).equals(v.getElements().get(pos2))) {
                pos1 += 1; pos2 += 1; olap += 1;
            } else if (u.getElements().get(pos1) < v.getElements().get(pos2)) {
                pos1 += 1; max1 -= 1;
            } else {
                pos2 += 1; max2 -= 1;
            }
        }
        return olap >= required;
    }
}
