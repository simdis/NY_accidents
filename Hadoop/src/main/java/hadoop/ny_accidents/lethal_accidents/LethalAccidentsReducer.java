package hadoop.ny_accidents.lethal_accidents;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import hadoop.ny_accidents.types.WeekYearWritable;

/**
 * The Reducer for LethalAccidents class.
 * @author Simone Disabato
 *
 */
public class LethalAccidentsReducer implements Reducer<WeekYearWritable,IntWritable,WeekYearWritable,IntWritable>{

    @Override
    public void configure(JobConf conf) {
        // Default implementation. Does nothing
    }

    @Override
    public void close() throws IOException {
        // Default implementation. Does nothing
    }

    @Override
    /**
     * The reduce function simply sum the tokens having the same key.
     */
    public void reduce(WeekYearWritable key, Iterator<IntWritable> values, OutputCollector<WeekYearWritable, 
            IntWritable> output, Reporter reporter) throws IOException {
        int sum = 0;
        while(values.hasNext()) {
            sum += values.next().get();
        }
        output.collect(key, new IntWritable(sum));
    }

}
