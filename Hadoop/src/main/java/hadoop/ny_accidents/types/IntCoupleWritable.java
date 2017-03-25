package hadoop.ny_accidents.types;

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
public class IntCoupleWritable implements WritableComparable<IntCoupleWritable> {

    private IntWritable int1;
    private IntWritable int2;


    public IntCoupleWritable() {
        this.int1 = new IntWritable();
        this.int2 = new IntWritable();
    }

    public IntCoupleWritable(int int1, int int2) {
        this.int1 = new IntWritable(int1);
        this.int2 = new IntWritable(int2);
    }


    @Override
    public void readFields(DataInput in) throws IOException {
        this.int1.readFields(in);
        this.int2.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.int1.write(out);
        this.int2.write(out);
    }

    @Override
    public int compareTo(IntCoupleWritable obj) {
        if (obj == null) {
            throw new NullPointerException();
        }
        if (this.equals(obj)) {
            return 0;
        }
        // The objects are sorted only on the int1 field
        if(this.int1.get()>obj.int1.get())
        {
            return 1;
        }
        if(this.int1.get()<obj.int1.get())
        {
            return -1;
        }
        else
        {
            return this.int2.compareTo(obj.int2);
        }
    }

    @Override
    public int hashCode() {
        final int prime = 1321;
        int result = 1;
        result = prime * result + ((this.int1 == null) ? 0 : this.int1.hashCode());
        result = prime * result + ((this.int2 == null) ? 0 : this.int2.hashCode());
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
        if (!(obj instanceof IntCoupleWritable)) {
            return false;
        }
        IntCoupleWritable other = (IntCoupleWritable) obj;
        if (this.int1 == null) {
            if (other.int1 != null) {
                return false;
            }
        } else if (!this.int1.equals(other.int1)) {
            return false;
        }
        if (this.int2 == null) {
            if (other.int2 != null) {
                return false;
            }
        } else if (!this.int2.equals(other.int2)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return this.int1 + "," + this.int2;
    }

    public IntWritable getSecond() {
        return this.int2;
    }

    public IntWritable getFirst()
    {
        return this.int1;
    }
}
