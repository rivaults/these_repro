package writables;

import writables.configuration.TrajectoryConfigurable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class TrajectoryWritable extends IdentifiedWritable {

    private final List<Double> points;
    private int size;

    public TrajectoryWritable(){
        points = new ArrayList<>();
    }

    public TrajectoryWritable(long id, List<Double> pts){
        super(id);
        this.points = pts;
        this.size = pts.size()/ TrajectoryConfigurable.DIMENSION;
    }

    public TrajectoryWritable(TrajectoryWritable from){
        super(from.getId());
        this.points = new ArrayList<>(from.getPoints().size());
        this.points.addAll(from.points);
        this.size = from.size;
    }

    public List<Double> getPoints() {
        return points;
    }

    public Double getCoord(int i, int d) {
        return points.get(i* TrajectoryConfigurable.DIMENSION+d);
    }


    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeInt(points.size());
        for (Double point : points)
            out.writeDouble(point);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        points.clear();
        int capacity = in.readInt();
        for(int i = 0; i < capacity; ++i)
            points.add(in.readDouble());
        size = capacity/ TrajectoryConfigurable.DIMENSION;
    }

    @Override
    public IdentifiedWritable copy() {
        return new TrajectoryWritable(this);
    }

    @Override
    public byte[] getBytes() {
        ByteBuffer buff = ByteBuffer.allocate(points.size() * 8);
        for(double e : points)
            buff.putDouble(e);
        return buff.array();
    }

    @Override
    public int getLength() {
        return getBytes().length;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TrajectoryWritable that = (TrajectoryWritable) o;
        return  super.equals(o) &&
                points.equals(that.points);
    }

    public int size() {
        return size;
    }

    public List<Double> getPoint(int i) {
        return points.subList(i* TrajectoryConfigurable.DIMENSION, i* TrajectoryConfigurable.DIMENSION+ TrajectoryConfigurable.DIMENSION);
    }

    static{
        conf = new TrajectoryConfigurable();
    }
}
