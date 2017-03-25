package hadoop.ny_accidents.types;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Writable class for tuple <borough, weekYear>
 * Created by fusiled on 02/03/17.
 */
public class BoroughWeekWritable implements WritableComparable<BoroughWeekWritable> {


    private Text borough;
    private WeekYearWritable wyw;

    public BoroughWeekWritable() {
        this.borough = new Text();
        this.wyw = new WeekYearWritable();
    }


    public BoroughWeekWritable(Text borText, WeekYearWritable wyw )
    {
        this.borough = borText;
        this.wyw = wyw;
    }

    public BoroughWeekWritable(String borough, WeekYear wy) {
        this.borough = new Text(borough);
        this.wyw = new WeekYearWritable(wy);
    }

    @Override
    public int compareTo(BoroughWeekWritable bww) {
        if (this.borough.equals(bww.borough)) {
            if (this.wyw.equals(bww.wyw)) {
                return 0;
            } else {
                return this.wyw.compareTo(bww.wyw);
            }
        } else {
            return this.borough.compareTo(bww.borough);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.borough.write(out);
        this.wyw.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.borough.readFields(in);
        this.wyw.readFields(in);
    }

    @Override
    public int hashCode() {
        final int prime = 317;
        int result = 3;
        result = prime * result + ((this.borough == null) ? 0 : this.borough.hashCode());
        result = prime * result + ((this.wyw == null) ? 0 : this.wyw.hashCode());
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
        if (!(obj instanceof BoroughWeekWritable)) {
            return false;
        }
        BoroughWeekWritable other = (BoroughWeekWritable) obj;
        if (this.borough == null) {
            if (other.borough != null) {
                return false;
            }
        } else if (!this.borough.equals(other.borough)) {
            return false;
        }
        if (this.wyw == null) {
            if (other.wyw != null) {
                return false;
            }
        } else if (!this.wyw.equals(other.wyw)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return this.borough + "\t" + this.wyw;
    }

    public Text getBorough() {
        return borough;
    }
}
