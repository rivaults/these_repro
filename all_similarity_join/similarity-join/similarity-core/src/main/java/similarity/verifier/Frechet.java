package similarity.verifier;

import org.apache.hadoop.conf.Configuration;
import writables.TrajectoryWritable;

import java.util.ArrayList;
import java.util.List;

import static similarity.verifier.EuclideanUtils.*;
import static utils.SetupTools.*;
import static writables.configuration.TrajectoryConfigurable.DIMENSION;

public class Frechet extends DistanceFields<TrajectoryWritable> {

    private static double THRESHOLD_SQR;
    private static final double epsilon = 1e-10;


    public static double getDistanceBetween(TrajectoryWritable t, int i, int j) {
        double res = 0d;
        for(int idx = i+1; idx <= j; ++idx){
            res += euclidean(t.getPoint(idx - 1), t.getPoint(idx));
        }
        return res;
    }

    /*
    create a traversal of the two curves moving at the same speed on both
    @returns boolean :  true : similarity between t1,t2 <= Threshold
                        false : we don't know
     */
    private static boolean equalTime(TrajectoryWritable t1, TrajectoryWritable t2, double threshold_sqr) {
        int i, j;
        i = j = 0;
        double distance = euclideanSqr(t1.getPoint(i), t2.getPoint(j));
        while (distance <= threshold_sqr && i + j < t1.size() + t2.size() - 2) {
            if (i == t1.size() - 1)
                distance = euclideanSqr(t1.getPoint(i), t2.getPoint(++j));
            else if (j == t2.size() - 1)
                distance = euclideanSqr(t1.getPoint(++i), t2.getPoint(j));
            else
                distance = euclideanSqr(t1.getPoint(++i), t2.getPoint(++j));
        }
        return distance <= threshold_sqr;
    }

    /*
    create a traversal of t1, t2
    @returns boolean :  true : similarity between t1,t2 <= Threshold
                        false : we don't know
     */
    private static boolean greedy(TrajectoryWritable t1, TrajectoryWritable t2, double threshold_sqr){
        int i, j;
        i = j = 0;
        double distance = euclideanSqr(t1.getPoint(i), t2.getPoint(j));
        double dist10, dist01, dist11;
        while (distance <= threshold_sqr && i + j < t1.size() + t2.size() - 2) {
            if (i == t1.size() - 1)
                distance = euclideanSqr(t1.getPoint(i), t2.getPoint(++j));
            else if (j == t2.size() - 1)
                distance = euclideanSqr(t1.getPoint(++i), t2.getPoint(j));
            else {
                /*
                    after matching vertices t1_i and t2_j,
                    we match t1_i′ and t2_j′,
                    for (i′,j′) ∈ {(i+1,j),(i,j+1),(i+1,j+1)}
                    minimizing |t1_i′−t2_j'|_2
                 */
                dist10 = euclideanSqr(t1.getPoint(i+1), t2.getPoint(j));
                dist01 = euclideanSqr(t1.getPoint(i), t2.getPoint(j+1));
                dist11 = euclideanSqr(t1.getPoint(i+1), t2.getPoint(j+1));
                if (dist10 < dist01 && dist10 < dist11){
                    distance = dist10;
                    ++i;
                }
                else if (dist01 < dist11){
                    distance = dist01;
                    ++j;
                }
                else {
                    distance = dist11;
                    ++i;
                    ++j;
                }
            }
        }
        return distance <= threshold_sqr;
    }


    private static int getNextPoint(TrajectoryWritable t, int i, List<Double> p, double threshold) {
        int delta = 1;
        while (true){
            if (i == t.size()-1)
                return (euclideanSqr(t.getPoint(i), p) <= threshold * threshold) ? i : t.size();
            else {
                delta = Math.min(delta, t.size()-1-i);
                if (euclidean(t.getPoint(i), p) - getDistanceBetween(t, i, i+delta) > threshold){
                    i += delta;
                    delta *= 2;
                }
                else if (delta > 1)
                    delta /= 2;
                else
                    return i;
            }
        }
    }

    private static boolean negative(TrajectoryWritable t1, TrajectoryWritable t2, double threshold) {
        int i, j;
        for (int delta = Math.max(t1.size(), t2.size())-1; delta >= 1; delta /= 2){
            i = 0;
            for(j = 0; j < t2.size(); j += delta){
                i = getNextPoint(t1, i, t2.getPoint(j), threshold);
                if (i >= t1.size())
                    return true;
            }
            j = 0;
            for(i = 0; i < t1.size(); i += delta){
                j = getNextPoint(t2, j, t1.getPoint(i), threshold);
                if (j >= t2.size())
                    return true;
            }
        }
        return false;
    }

    private static final List<Pair> r1 = new ArrayList<>(64);
    private static final List<Pair> r2 = new ArrayList<>(64);
    private static final List<Pair> r1Out = new ArrayList<>(64);
    private static final List<Pair> r2Out = new ArrayList<>(64);

    /*
    Recursive algorithm proposed by Julian Baldus and Karl Bringmann in paper :
    A fast implementation of near neighbors queries for Fréchet distance (GIS Cup)
     */
    private static boolean full(TrajectoryWritable t1, TrajectoryWritable t2, double threshold) {
        r1.clear();
        r2.clear();
        r1Out.clear();
        r2Out.clear();
        double threshold_sqr = sqr(threshold);
        r1.add(new Pair(0d, getLastReachablePoint(t1, t2, threshold_sqr)));
        r2.add(new Pair(0d, getLastReachablePoint(t2, t1, threshold_sqr)));
        recursiveReach(t1, t2, 0, t1.size()-1, 0, t2.size()-1, r1, r2, r1Out, r2Out, threshold);
        return r1Out.size() > 0 && (r1Out.get(r1Out.size()-1).second + epsilon >= t2.size() - 1d);
    }

    /*
    Used only for instantiate, so it's from first point of first parameter to second parameter
     */
    private static double getLastReachablePoint(TrajectoryWritable from, TrajectoryWritable to, double threshold_sqr) {
        int j = 0;
        while(j < to.size()-2 && euclideanSqr(from.getPoint(0), to.getPoint(j+1)) <= threshold_sqr)
            ++j;
        return reachDirection(from, to, 0, j, threshold_sqr).second;
    }


    public static double[] minus(List<Double> p, List<Double> o) {
        double[] res = new double[DIMENSION];
        for(int d = 0; d < DIMENSION; ++d)
            res[d] = p.get(d) - o.get(d);
        return res;
    }

    public static double[] minus(double[] a, double[] b) {
        double[] res = new double[DIMENSION];
        for(int d = 0; d < DIMENSION; ++d)
            res[d] = a[d] - b[d];
        return res;
    }

    private static Pair intersect(List<Double> center, List<Double> lineStart, List<Double> lineEnd, double threshold_sqr){
        double[] start = minus(lineStart, center);
        double[] end = minus(lineEnd, center);
        double[] v = minus(end, start);
        double a, b, c;
        a = b = c = 0d;
        for (int i = 0; i < DIMENSION; ++i) {
            a += sqr(v[i]);
            b += start[i] * v[i];
            c += sqr(start[i]);
        }
        c -= threshold_sqr;
        double discriminant = sqr(b/a) - c/a;
        if (discriminant < 0)
            return PAIR_DEFAULT;
        double lambdaA = -b/a - Math.sqrt(discriminant);
        double lambdaB = -b/a + Math.sqrt(discriminant);
        if (lambdaB < 0 || lambdaA > 1)
            return PAIR_DEFAULT;
        double first = Math.max(lambdaA, 0);
        double second = Math.min(lambdaB, 1);
        return new Pair(first, second);
    }

    private static boolean getDistFromPoint(TrajectoryWritable t2, List<Double> p) {
        for(int i = 0; i < t2.size(); ++i) {
            if (euclideanSqr(p, t2.getPoint(i)) > THRESHOLD_SQR)
                return false;
        }
        return true;
    }

    private static Pair reachDirection(TrajectoryWritable t1, TrajectoryWritable t2, int i, int j, double threshold_sqr){
        Pair res = intersect(t1.getPoint(i), t2.getPoint(j), t2.getPoint(j+1), threshold_sqr);
        res.first = res.first + j;
        res.second = res.second + j;
        return res;
    }

    private static void merge(List<Pair> list, Pair elt){
        if (!elt.isEmpty()) {
            if (list.size() > 0 && (elt.first - epsilon <= list.get(list.size()-1).second))
                list.get(list.size()-1).second = elt.second;
            else
                list.add(elt);
        }
    }
    private static void recursiveReach(TrajectoryWritable t1, TrajectoryWritable t2,
                                       int iMin, int iMax, int jMin, int jMax,
                                       List<Pair> rB, List<Pair> rA,
                                       List<Pair> rBOut, List<Pair> rAOut,
                                       double threshold){
        double threshold_sqr = sqr(threshold);
        Pair itemA = getLastItemLower(rA, iMax, iMin);
        Pair itemB = getLastItemLower(rB, jMax, jMin);
        if (itemA.isEmpty() && itemB.isEmpty())
            return;
        if (itemA.first <= iMin + epsilon && itemA.second >= iMax - epsilon &&
                itemB.first <= jMin + epsilon && itemB.second >= jMax - epsilon) {
            int iMid = (iMin+1+iMax)/2;
            int jMid = (jMin+1+jMax)/2;
            if (euclidean(t1.getPoint(iMid), t2.getPoint(jMid)) +
                    Math.max(getDistanceBetween(t1, iMin+1, iMid), getDistanceBetween(t1, iMid, iMax)) +
                    Math.max(getDistanceBetween(t2,jMin+1, jMid), getDistanceBetween(t2, jMid, jMax)) <= threshold){
                merge(rAOut, new Pair(iMin, iMax));
                merge(rBOut, new Pair(jMin, jMax));
                return;
            }
        }
        if (iMin == iMax-1 && jMin == jMax-1){
            Pair reachableA = reachDirection(t1, t2, iMax, jMin, threshold_sqr);
            Pair reachableB = reachDirection(t2, t1, jMax, iMin, threshold_sqr);
            if (itemA.isEmpty())
                reachableA.first = Math.max(reachableA.first, itemB.first);
            else if (itemB.isEmpty())
                reachableB.first = Math.max(reachableB.first, itemA.first);
            merge(rBOut, reachableA);
            merge(rAOut, reachableB);
        }
        else if (jMax - jMin > iMax - iMin){
            List<Pair> rAMid = new ArrayList<>();
            int jSplit = (jMax+jMin)/2;
            recursiveReach(t1, t2, iMin, iMax, jMin, jSplit, rB, rA, rBOut, rAMid, threshold);
            recursiveReach(t1, t2, iMin, iMax, jSplit, jMax, rB, rAMid, rBOut, rAOut, threshold);
        }
        else {
            List<Pair> rBMid = new ArrayList<>();
            int iSlit = (iMax+iMin)/2;
            recursiveReach(t1, t2, iMin, iSlit, jMin, jMax, rB, rA, rBMid, rAOut, threshold);
            recursiveReach(t1, t2, iSlit, iMax, jMin, jMax, rBMid, rA, rBOut, rAOut, threshold);
        }
    }

    private static Pair getLastItemLower(List<Pair> l, int pivotFirst, int pivotSecond) {
        int i = 0;
        for(; i < l.size(); ++i){
            if (l.get(i).first > pivotFirst)
                break;
        }
        if (i != 0){
            --i;
            if (l.get(i).first <= pivotFirst && l.get(i).second >= pivotSecond)
                return l.get(i);
        }
        return PAIR_DEFAULT;
    }

    private Boolean verifyHeuristics(TrajectoryWritable t1, TrajectoryWritable t2, double t){
        double t_sqr = sqr(t);
        if (equalTime(t1, t2, t_sqr) || greedy(t1, t2, t_sqr)) {
            return true;
        }
        if (negative(t1, t2, t)) {
            return false;
        }
        return null;
    }

    @Override
    public boolean verify(TrajectoryWritable t1, TrajectoryWritable t2){
        if (euclideanSqr(t1.getPoint(0), t2.getPoint(0)) > THRESHOLD_SQR ||
                euclideanSqr(t1.getPoint(t1.size()-1), t2.getPoint(t2.size()-1)) > THRESHOLD_SQR)
            return false;
        if (t1.size() == 1 && t2.size() == 1)
            return true;
        if (t1.size() == 1)
            return getDistFromPoint(t2, t1.getPoint(0));
        if (t2.size() == 1)
            return getDistFromPoint(t1, t2.getPoint(0));
        Boolean heuristics = verifyHeuristics(t1, t2, THRESHOLD);
        if (heuristics != null)
            return heuristics;
        return full(t1, t2, THRESHOLD);
    }

    private final static Pair PAIR_DEFAULT = new Pair();

    @Override
    public boolean verify(TrajectoryWritable u, TrajectoryWritable v, double t) {
        return false;
    }

    @Override
    public void configure(Configuration c) {
        THRESHOLD_SQR = sqr(THRESHOLD);
    }

    @Override
    public boolean sizeFilter(short length, short length1) {
        return true;
    }

    static class Pair{
        private double first;
        private double second;

        Pair(){
            first = Double.MAX_VALUE;
            second = Double.MIN_VALUE;
        }

        Pair(double k, double v){
            first = k;
            second = v;
        }

        public boolean isEmpty(){
            return first >= second;
        }

        @Override
        public String toString() {
            return "Pair{" +
                    "first=" + first +
                    ", second=" + second +
                    '}';
        }
    }
}
