package similarity.verifier;

import java.util.List;

import static writables.configuration.TrajectoryConfigurable.DIMENSION;

public class EuclideanUtils {

    public static double sqr(double x){
        return x * x;
    }

    public static double euclideanSqr(List<Double> a, List<Double> b) {
        double dist = 0;
        for(int d = 0; d < DIMENSION; ++d)
            dist += sqr(a.get(d) - b.get(d));
        return dist;
    }

    public static double euclidean(List<Double> a, List<Double> b) {
        return Math.sqrt(euclideanSqr(a, b));
    }
}
