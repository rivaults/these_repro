package writables.configuration;

import org.apache.hadoop.conf.Configuration;
import utils.Configurable;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class TrajectoryConfigurable implements Configurable {

    public static int DIMENSION = 2;

    @Override
    public void setup(Configuration c) {
        DIMENSION = c.getInt("trajectory.dimension", DIMENSION);
    }
}
