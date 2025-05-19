package similarity.verifier;

import org.apache.hadoop.conf.Configuration;
import writables.TrajectoryWritable;


import static similarity.verifier.EuclideanUtils.euclidean;
import static utils.SetupTools.THRESHOLD;

public class DTW extends DistanceFields<TrajectoryWritable>{

    private double minimum(double[] l, int... indices){
        double min = Double.MAX_VALUE;
        for(int i : indices)
            if (l[i] < min)
                min = l[i];
        return min;
    }

    private double full(TrajectoryWritable u, TrajectoryWritable v){
        int uSize = u.size();
        int vSize = v.size();
        double[] res = new double[uSize*vSize];
        for(int i = 0; i < uSize*vSize; ++i)
            res[i] = Double.MAX_VALUE;
        res[0] = 0d;
        for(int i = 1; i < uSize; ++i) {
            for (int j = 1; j < vSize; ++j) {
                res[i * vSize + j] = euclidean(
                        u.getPoint(i),
                        v.getPoint(j)
                );
                res[i * vSize + j] += minimum(
                        res,
                        (i - 1) * vSize + j,
                        i * vSize + (j - 1),
                        (i - 1) * vSize + (j - 1)
                );
            }
        }
        return res[(uSize-1)*(vSize-1)];
    }

    @Override
    public boolean verify(TrajectoryWritable u, TrajectoryWritable v) {
        return full(u, v) <= THRESHOLD;
    }

    @Override
    public boolean verify(TrajectoryWritable u, TrajectoryWritable v, double t) {
        return full(u, v) <= t;
    }

    @Override
    public void configure(Configuration c) {}

    @Override
    public boolean sizeFilter(short length, short length1) {
        return true;
    }
}
