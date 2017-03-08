package hadoop.ny_accidents.types;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Wraps a couple of a int and a float in their Writable associated classes and into a single class.
 *
 * @author fusiled
 *         Modified from SumAndAverageWritable
 */
public class IntFloatCoupleWritable implements WritableComparable<IntFloatCoupleWritable> {

    // Minimum number of digits used to display the intV attribute in toString()
    private static final int intVDigits = 7;
    private IntWritable intV;
    private FloatWritable floatV;


    public IntFloatCoupleWritable() {
        this.intV = new IntWritable();
        this.floatV = new FloatWritable();
    }

    public IntFloatCoupleWritable(int intV, float floatV) {
        this.intV = new IntWritable(intV);
        this.floatV = new FloatWritable(floatV);
    }


    @Override
    public void readFields(DataInput in) throws IOException {
        this.intV.readFields(in);
        this.floatV.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.intV.write(out);
        this.floatV.write(out);
    }

    @Override
    public int compareTo(IntFloatCoupleWritable obj) {
        if (obj == null) {
            throw new NullPointerException();
        }
        if (this.equals(obj)) {
            return 0;
        }
        // The objects are sorted only on the intV field
        int intVCompare = this.intV.compareTo(obj.intV);
        if (intVCompare == 0) {
            // In this special case the ordering is defined over the floatV field
            return this.floatV.compareTo(obj.floatV);
        }
        return intVCompare;
    }

    @Override
    public int hashCode() {
        final int prime = 89;
        int result = 1;
        result = prime * result + ((this.intV == null) ? 0 : this.intV.hashCode());
        result = prime * result + ((this.floatV == null) ? 0 : this.floatV.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof IntFloatCoupleWritable)) {
            return false;
        }
        IntFloatCoupleWritable other = (IntFloatCoupleWritable) obj;
        if (this.intV == null) {
            if (other.intV != null) {
                return false;
            }
        } else if (!this.intV.equals(other.intV)) {
            return false;
        }
        if (this.floatV == null) {
            if (other.floatV != null) {
                return false;
            }
        } else if (!this.floatV.equals(other.floatV)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return this.intV + "," + this.floatV;
    }
}
