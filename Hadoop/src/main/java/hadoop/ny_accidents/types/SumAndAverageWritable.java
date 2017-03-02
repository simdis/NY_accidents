package hadoop.ny_accidents.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

/**
 * A simple writable class, i.e., usable in Hadoop jobs, that saves an integer
 * and a percentage. Moreover it can store two strings that are used as the description
 * of the attributes in the toString() method.
 * Note that the percentage is expressed in the toString() method as per mille or percentage, 
 * whereas it is set and accessed as a double between 0 and 1.
 * @author Simone Disabato
 *
 */
public class SumAndAverageWritable implements WritableComparable<SumAndAverageWritable>{
    
    private IntWritable sum;
    private DoubleWritable average;
    private String sumName;
    private String percentageName;
    private DecimalFormat percentageFormat = new DecimalFormat("###.###\u2030");
    // Minimum number of digits used to display the sum attribute in toString()
    private static final int sumDigits = 7;
    

    public SumAndAverageWritable () {
        this.sum = new IntWritable();
        this.average = new DoubleWritable();
        this.sumName = "Accidents";
        this.percentageName = "Lethal percentage";
    }
    
    public SumAndAverageWritable(int sum, double average) {
        this.sum = new IntWritable(sum);
        this.average = new DoubleWritable(average);
        this.sumName = "Accidents";
        this.percentageName = "Lethal percentage";
    }
    
    public SumAndAverageWritable(int sum, double average, String sumName, String percentageName) {
        this.sum = new IntWritable(sum);
        this.average = new DoubleWritable(average);
        this.sumName = sumName;
        this.percentageName = percentageName;
    }
    
    public IntWritable getSum() {
        return sum;
    }

    public void setSum(IntWritable sum) {
        this.sum = sum;
    }

    public DoubleWritable getAverage() {
        return average;
    }

    public void setAverage(DoubleWritable average) {
        this.average = average;
    }
   
    public String getSumName() {
        return sumName;
    }

    public void setSumName(String sumName) {
        this.sumName = sumName;
    }

    public String getPercentageName() {
        return percentageName;
    }

    public void setPercentageName(String percentageName) {
        this.percentageName = percentageName;
    }
    
    /**
     * Set the display format of the percentage. It can be either a percentage or per mille
     * @param percentage boolean that allows to switch between percentage format (true) and
     * per mille (false)
     */
    public void setPercentage(boolean percentage) {
        if (percentage == true) {
            percentageFormat = new DecimalFormat("###.###%");
        } else {
            percentageFormat = new DecimalFormat("###.###\u2030");
        }
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        this.sum.readFields(in);
        this.average.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.sum.write(out);
        this.average.write(out);
    }
    
    @Override
    public int compareTo(SumAndAverageWritable obj) {
        if (obj == null) {
            throw new NullPointerException();
        }
        if (this.equals(obj)) {
            return 0;
        }
        // The objects are sorted only on the sum field
        int sumCompare = this.sum.compareTo(obj.sum);
        if (sumCompare == 0) {
            // In this special case the ordering is defined over the average field
            return this.average.compareTo(obj.average); 
        }
        return sumCompare;
    }
    
    @Override
    public int hashCode() {
        final int prime = 47;
        int result = 1;
        result = prime * result + ((this.sum == null) ? 0 : this.sum.hashCode());
        result = prime * result + ((this.average == null) ? 0 : this.average.hashCode());
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
        if (!(obj instanceof SumAndAverageWritable)) {
            return false;
        }
        SumAndAverageWritable other = (SumAndAverageWritable) obj;
        if (this.sum == null) {
            if (other.sum != null) {
                return false;
            }
        }
        else if (!this.sum.equals(other.sum)) {
            return false;
        }
        if (this.average == null) {
            if (other.average != null) {
                return false;
            }
        }
        else if (!this.average.equals(other.average)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return this.sumName + ": " + StringUtils.leftPad(this.sum.toString(), sumDigits)
                + "\t" + this.percentageName + ": " 
                + percentageFormat.format(this.average.get());
    }
}
