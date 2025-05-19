package writables.partitionned;

import writables.TrajectoryWritable;

public class TrajectoryKeysWritable extends GenericKeysObjectWritable{
    public TrajectoryKeysWritable() {
        super(TrajectoryWritable.class);
    }
}
