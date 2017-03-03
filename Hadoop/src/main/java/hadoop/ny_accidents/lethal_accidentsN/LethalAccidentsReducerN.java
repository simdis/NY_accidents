package hadoop.ny_accidents.lethal_accidentsN;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import hadoop.ny_accidents.types.WeekYearWritable;

/**
 * The Reducer for LethalAccidents class.
 * The version N is developed according to new API (org.hadoop.mapreduce).
 * @author Simone Disabato
 *
 */
public class LethalAccidentsReducerN extends Reducer<WeekYearWritable,IntWritable,WeekYearWritable,IntWritable>{

    @Override
    public void cleanup(Context context) throws IOException {
        // Default implementation. Does nothing
    }

    @Override
    /**
     * The reduce function simply sum the tokens having the same key.
     */
    public void reduce (WeekYearWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        Iterator<IntWritable> valuesIterator = values.iterator();
        while(valuesIterator.hasNext()) {
            sum += valuesIterator.next().get();
        }
        context.write(key, new IntWritable(sum));
    }

}
