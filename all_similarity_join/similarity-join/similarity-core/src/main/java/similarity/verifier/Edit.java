package similarity.verifier;

import org.apache.hadoop.conf.Configuration;
import writables.SequenceWritable;

import static utils.SetupTools.*;
import static writables.configuration.SequenceConfigurable.MAX_SEQ;

public class Edit extends DistanceFields<SequenceWritable> {


    private static final int [] EMPTY_INT_ARRAY = new int[0];
    private int[] currentLeft = EMPTY_INT_ARRAY;
    private int[] currentRight = EMPTY_INT_ARRAY;
    private int[] lastLeft = EMPTY_INT_ARRAY;
    private int[] lastRight = EMPTY_INT_ARRAY;
    private int[] priorLeft = EMPTY_INT_ARRAY;
    private int[] priorRight = EMPTY_INT_ARRAY;

    @Override
    public boolean verify(SequenceWritable u, SequenceWritable v) {
        return verify(u, v, THRESHOLD);
    }

    @Override
    public boolean verify(SequenceWritable u, SequenceWritable v, double t) {
        return distanceLoE(u, v, t) <= t;
    }

    public double distanceLoE(SequenceWritable l, SequenceWritable r, double t) {
        final int uL = l.getSequence().getLength();
        final int vL = r.getSequence().getLength();
        final int main = uL - vL;

        int distance = Math.abs(main);
        if (distance > t) {
            return Integer.MAX_VALUE;
        }

        if (main <= 0) {
            ensureCapacityRight(distance, false);
            for (int j = 0; j <= distance; j++) {
                lastRight[j] = distance - j - 1;
                priorRight[j] = -1;
            }
        } else {
            ensureCapacityLeft(distance, false);
            for (int j = 0; j <= distance; j++) {
                lastLeft[j] = -1;
                priorLeft[j] = -1;
            }
        }

        boolean even = true;

        while (true) {
            int offDiagonal = (distance - main) / 2;
            ensureCapacityRight(offDiagonal, true);
            if (even) {
                lastRight[offDiagonal] = -1;
            }
            int immediateRight = -1;
            for (; offDiagonal > 0; offDiagonal--) {
                currentRight[offDiagonal] = immediateRight = computeRow(
                        (main + offDiagonal),
                        (distance - offDiagonal),
                        l, r,
                        uL, vL,
                        priorRight[offDiagonal - 1],
                        lastRight[offDiagonal],
                        immediateRight);
            }
            offDiagonal = (distance + main) / 2;
            ensureCapacityLeft(offDiagonal, true);
            if (even) {
                lastLeft[offDiagonal] = (distance - main) / 2 - 1;
            }
            int immediateLeft = even ? -1 : (distance - main) / 2;
            for (; offDiagonal > 0; offDiagonal--) {
                currentLeft[offDiagonal] = immediateLeft = computeRow(
                        (main - offDiagonal),
                        (distance - offDiagonal),
                        l, r,
                        uL, vL,
                        immediateLeft,
                        lastLeft[offDiagonal],
                        priorLeft[offDiagonal - 1]);
            }
            int mainRow = computeRow(main, distance, l, r, uL, vL,
                    immediateLeft, lastLeft[0], immediateRight);
            if ((mainRow == vL) || (++distance > t) || (distance < 0)) {
                break;
            }
            currentLeft[0] = currentRight[0] = mainRow;
            int[] tmp;
            priorLeft = lastLeft;
            lastLeft = currentLeft;
            currentLeft = priorLeft;

            tmp = priorRight;
            priorRight = lastRight;
            lastRight = currentRight;
            currentRight = tmp;
            even = !even;
        }
        return distance;
    }

    private void ensureCapacityLeft(int index, boolean copy) {
        if (currentLeft.length <= index) {
            index++;
            priorLeft = resize(priorLeft, index, copy);
            lastLeft = resize(lastLeft, index, copy);
            currentLeft = resize(currentLeft, index, false);
        }
    }

    private void ensureCapacityRight(int index, boolean copy) {
        if (currentRight.length <= index) {
            index++;
            priorRight = resize(priorRight, index, copy);
            lastRight = resize(lastRight, index, copy);
            currentRight = resize(currentRight, index, false);
        }
    }

    private int[] resize(int[] array, int size, boolean copy) {
        int[] result = new int[size];
        if (copy) {
            System.arraycopy(array, 0, result, 0, array.length);
        }
        return result;
    }

    private int computeRow(int k, int p, SequenceWritable a, SequenceWritable b, int aL, int bL,
                           int knownLeft, int knownAbove, int knownRight) {
        assert (Math.abs(k) <= p);
        assert (p >= 0);
        int t;
        if (p == 0) {
            t = 0;
        } else {
            t = Math.max(Math.max(knownAbove, knownRight) + 1, knownLeft);
        }
        int tMax = Math.min(bL, (aL - k));
        while ((t < tMax) && b.getByte(t) == a.getByte(t + k)) {
            t++;
        }
        return t;
    }

    @Override
    public void configure(Configuration c) {}

    @Override
    public boolean sizeFilter(short length, short length1) {
        return true;
    }
}
