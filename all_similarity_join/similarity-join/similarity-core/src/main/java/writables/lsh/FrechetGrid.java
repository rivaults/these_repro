package writables.lsh;

import org.apache.hadoop.conf.Configuration;
import utils.Hash;
import writables.IdentifiedWritable;
import writables.TrajectoryWritable;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import static utils.SetupTools.*;
import static writables.configuration.TrajectoryConfigurable.DIMENSION;

public class FrechetGrid extends LSH {

    public static double[] GRID_SIDE = new double[]{
            4*THRESHOLD,
            4*THRESHOLD
    };

    private double[] origin;

    @Override
    public void configure(Configuration c, boolean estimator){
        super.configure(c, estimator);
        String side = c.get("lsh.grids.sides");
        if (side != null){
            String[] dimsSide = side.split(",");
            for(int i = 0; i < dimsSide.length; ++i)
                GRID_SIDE[i] = Double.parseDouble(dimsSide[i]);
        }
        origin = new double[DIMENSION];
        for(int i = 0; i < DIMENSION; ++i)
            origin[i] = rnd.nextDouble() * GRID_SIDE[i];
    }

    public FrechetGrid() {}

    public FrechetGrid(int id, DataInput rnd, int k, int kInner) throws IOException {
        super(id, rnd, k, kInner);
    }

    /*
    By the variable NB_GRID_BY_HASH, we may choose to represent a hash by multiple grid
    For each sub grids :
    Steps :
        1/ snap trajectory node to the nearest grid node
        2/ add its hash in the list if the grid node is different from the latest.
        3/ Repeat 1-2 for each node in the trajectory
        4/ hash the list
    @params Trajectory Writable t : a trajectory
    @returns Long : bytes of the trajectory's hash are 0's between position 0 to 32, we shift the index in these positions
     */
    public void digest(IdentifiedWritable o) {
        rnd.setSeed(seed);
        TrajectoryWritable t = (TrajectoryWritable) o;
        long seedDims = rnd.nextLong();
        ArrayList<Integer> inner = new ArrayList<>(64);
        long[] current = new long[DIMENSION];
        long[] previous = new long[DIMENSION];
        for (int i = 0; i < t.size(); ++i) {
            for (int d = 0; d < DIMENSION; ++d) {
                double coordinate = (t.getCoord(i, d) - origin[d]) / GRID_SIDE[d];
                current[d] = Math.round(coordinate);
            }
            if (!Arrays.equals(current, previous)) {
                inner.add(Hash.digestLongs(seedDims, current));
                System.arraycopy(current, 0, previous, 0, DIMENSION);
            }
        }
        keys[0] = Hash.getHash(id, Hash.digestInts(seed, inner));
        inner.clear();
    }

    @Override
    public long hash() {
        return keys[0];
    }
}
