package KNN;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// WritableComparable class for a paired Double and String (distance and model)
// This is a custom class for MapReduce to pass a double and a String through context
// as one serializable object.
// This example only implements the minimum required methods to make this job run. To be
// deployed robustly is should include ToString(), hashCode(), WritableComparable interface
// if this object was intended to be used as a key etc.
public class DoubleString implements WritableComparable<DoubleString> {
    private Double distance = 0.0;
    private String model = null;

    public void set(Double lhs, String rhs) {
        distance = lhs;
        model = rhs;
    }

    public Double getDistance() {
        return distance;
    }

    public String getModel() {
        return model;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        distance = in.readDouble();
        model = in.readUTF();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(distance);
        out.writeUTF(model);
    }

    @Override
    public int compareTo(DoubleString o) {
        return (this.model).compareTo(o.model);

    }
}