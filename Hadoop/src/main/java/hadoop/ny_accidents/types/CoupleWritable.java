package hadoop.ny_accidents.types;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by fusiled on 14/03/17.
 */
public class CoupleWritable<T extends WritableComparable,K extends WritableComparable > implements WritableComparable<CoupleWritable<T,K>> {

    private T first;
    private K second;


    public CoupleWritable(T first, K second)
    {
        this.first=first;
        this.second=second;
    }

    public CoupleWritable()
    {
        this.first=null;
        this.second=null;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.first.write(out);
        this.second.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.first.readFields(in);
        this.second.readFields(in);
    }


    @Override
    public int compareTo(CoupleWritable obj) {
        if (obj == null) {
            throw new NullPointerException();
        }
        if (this.equals(obj)) {
            return 0;
        }
        // The objects are sorted only on the first field
        int firstCompare = this.first.compareTo(obj.first);
        if (firstCompare == 0) {
            // In this special case the ordering is defined over the second field
            return this.second.compareTo(obj.second);
        }
        return firstCompare;
    }

    @Override
    public int hashCode() {
        final int prime = 373;
        int result = 1;
        result = prime * result + ((this.first == null) ? 0 : this.first.hashCode());
        result = prime * result + ((this.second == null) ? 0 : this.second.hashCode());
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
        if (!(obj instanceof CoupleWritable)) {
            return false;
        }
        CoupleWritable other = (CoupleWritable) obj;
        if (this.first == null) {
            if (other.first != null) {
                return false;
            }
        } else if (!this.first.equals(other.first)) {
            return false;
        }
        if (this.second == null) {
            if (other.second != null) {
                return false;
            }
        } else if (!this.second.equals(other.second)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return this.first + "," + this.second;
    }


    public void setFirst(T first){ this.first = first;}

    public void setSecond(K second) {
        this.second = second;
    }

    public T getFirst() {
        return this.first;
    }

    public K getSecond() {
        return this.second;
    }
}
