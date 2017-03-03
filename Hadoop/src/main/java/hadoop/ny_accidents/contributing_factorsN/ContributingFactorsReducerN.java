package hadoop.ny_accidents.contributing_factorsN;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import hadoop.ny_accidents.types.SumAndAverageWritable;

/**
 * Reducer class for the contributing factors job.
 * The version N is developed according to new API (org.hadoop.mapreduce).
 * 
 * @author Simone Disabato
 *
 */
public class ContributingFactorsReducerN extends Reducer<Text, IntWritable, Text, SumAndAverageWritable> {

    @Override
    public void cleanup(Context context) throws IOException {
        // Default implementation. Does nothing
    }

    @Override
    /**
     * The reduce function count the incidents per contributing factor and, finally,
     * compute the average number of fatal accidents as count_fatal_accidents/count, where
     * the first term is the number of tokens having value greater than zero
     */
    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
            throws IOException, InterruptedException {
        int count = 0, countFatal = 0;
        Iterator<IntWritable> valuesIterator = values.iterator();
        while (valuesIterator.hasNext()) {
            count ++;
            if (valuesIterator.next().get() > 0) {
                countFatal ++;
            }
        }
        context.write(key, new SumAndAverageWritable(count, (double)countFatal/count));
    }
    
}
