package hadoop.ny_accidents.contributing_factors;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import hadoop.ny_accidents.types.SumAndAverageWritable;

public class ContributingFactorsReducer implements Reducer<Text, IntWritable, Text, SumAndAverageWritable> {

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
     * The reduce function count the incidents per contributing factor and, finally,
     * compute the average number of fatal accidents as count_fatal_accidents/count, where
     * the first term is the number of tokens having value greater than zero
     */
    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, 
            SumAndAverageWritable> output, Reporter reporter) throws IOException {
        int count = 0, countFatal = 0;
        while (values.hasNext()) {
            count ++;
            if (values.next().get() > 0) {
                countFatal ++;
            }
        }
        output.collect(key, new SumAndAverageWritable(count, (double)countFatal/count));
    }
    
}
