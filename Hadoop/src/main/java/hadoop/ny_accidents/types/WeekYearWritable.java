package hadoop.ny_accidents.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import hadoop.ny_accidents.util.WeekYear;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

/**
 * A simple class usable in Hadoop jobs. It represents a pair (year,week).
 * @author Simone Disabato
 *
 */
public class WeekYearWritable implements WritableComparable<WeekYearWritable> {
    private IntWritable week;
    private IntWritable year;

    public WeekYearWritable() {
        this.week = new IntWritable();
        this.year = new IntWritable();
    }
    
    public WeekYearWritable(int year, int week) {
        this.week = new IntWritable(week);
        this.year = new IntWritable(year);
    }

    public WeekYearWritable(WeekYear wy){
        this.week = new IntWritable(wy.getWeek());
        this.year = new IntWritable(wy.getYear());
    }
    
    public IntWritable getWeek() {
        return week;
    }

    public void setWeek(IntWritable week) {
        this.week = week;
    }

    public IntWritable getDate() {
        return year;
    }

    public void setDate(IntWritable year) {
        this.year = year;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.year.readFields(in);
        this.week.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.year.write(out);
        this.week.write(out);
    }

    @Override
    public int compareTo(WeekYearWritable obj) {
        if (obj == null) {
            throw new NullPointerException();
        }
        if (this.equals(obj)) {
            return 0;
        }
        if (this.year.compareTo(obj.year) < 0 || 
                (this.year.equals(obj.year) && this.week.compareTo(obj.week) < 0)) {
            return -1;
        }
        return +1;
    }
    
    @Override
    public int hashCode() {
        final int prime = 47;
        int result = 1;
        result = prime * result + ((this.year == null) ? 0 : this.year.hashCode());
        result = prime * result + ((this.week == null) ? 0 : this.week.hashCode());
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
        if (!(obj instanceof WeekYearWritable)) {
            return false;
        }
        WeekYearWritable other = (WeekYearWritable) obj;
        if (this.year == null) {
            if (other.year != null) {
                return false;
            }
        }
        else if (!this.year.equals(other.year)) {
            return false;
        }
        if (this.week == null) {
            if (other.week != null) {
                return false;
            }
        }
        else if (!this.week.equals(other.week)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return this.year + "\t" + this.week;
    }

}
