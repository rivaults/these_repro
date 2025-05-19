package writables.partitionned;

import writables.SequenceWritable;

public class SequenceKeysWritable extends GenericKeysObjectWritable{

    public SequenceKeysWritable() {
        super(SequenceWritable.class);
    }

}
