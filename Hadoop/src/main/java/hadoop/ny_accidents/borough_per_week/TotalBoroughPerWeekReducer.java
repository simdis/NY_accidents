package hadoop.ny_accidents.borough_per_week;

import hadoop.ny_accidents.types.BoroughWeekWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 *
 */
public class TotalBoroughPerWeekReducer extends Reducer<BoroughWeekWritable, IntWritable, BoroughWeekWritable, IntWritable> {

    @Override
    public void cleanup(Context context) throws IOException {
        // Default implementation. Does nothing
    }

    @Override
    /**
     * The reduce function simply sum the tokens having the same key.
     */
    public void reduce(BoroughWeekWritable key, Iterable<IntWritable> valuesIterable, Context context)
            throws InterruptedException, IOException {
        Iterator<IntWritable> values = valuesIterable.iterator();
        int sum = 0;
        while (values.hasNext()) {
            sum += values.next().get();
        }
        context.write(key, new IntWritable(sum));
    }

}
