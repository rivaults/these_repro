package writables.histogram;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import utils.Hash;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;

public class FrequencyWritable implements Writable {

    private int frequency;
    private final HashSet<Integer> partitions;

    public FrequencyWritable(){
        this.frequency = 1;
        this.partitions = new HashSet<>();
    }

    public FrequencyWritable(int split, int group){
        this.frequency = 1;
        this.partitions = new HashSet<>();
        this.partitions.add(Hash.digestSplitId(split, group));
    }

    public FrequencyWritable(FrequencyWritable from) {
        this.frequency = from.frequency;
        this.partitions = new HashSet<>();
        this.partitions.addAll(from.partitions);
    }

    public void setAll(int split, int group){
        this.frequency = 1;
        this.partitions.clear();
        this.partitions.add(Hash.digestSplitId(split, group));
    }

    public int getFrequency() {
        return frequency;
    }

    public HashSet<Integer> getPartitions() {
        return partitions;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeVInt(out, frequency);
        WritableUtils.writeVInt(out, partitions.size());
        for (int p : partitions) {
            WritableUtils.writeVInt(out, Hash.getSplitId(p));
            WritableUtils.writeVInt(out, Hash.getHistId(p));
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        partitions.clear();
        frequency = WritableUtils.readVInt(in);
        int sizeP = WritableUtils.readVInt(in);
        for(int i = 0; i < sizeP; ++i){
            int s = WritableUtils.readVInt(in);
            int g = WritableUtils.readVInt(in);
            partitions.add(Hash.digestSplitId(s, g));
        }
    }

    public void clear() {
        this.frequency = 0;
        this.partitions.clear();
    }

    public void union(FrequencyWritable that){
        partitions.addAll(that.partitions);
    }

    public void add(FrequencyWritable that){
        frequency += that.frequency;
    }

    public boolean shouldKeep() {
        return frequency > 1;
    }

    @Override
    public String toString() {
        return "FrequencyWritable{" +
                "frequency=" + frequency +
                ", partitions=" + partitions +
                '}';
    }
}