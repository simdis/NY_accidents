package hadoop.ny_accidents.borough_per_week;

import hadoop.ny_accidents.types.IntFloatCoupleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Reducer class. It computes the total number of lethal accidents foreach borough and it also compute the average.
 * The number of weeks is obtained by Configuration attributes
 *
 * @author fusiled
 */
public class AverageBoroughPerWeekReducer extends Reducer<Text, IntWritable, Text, IntFloatCoupleWritable> {

    @Override
    public void cleanup(Context context) throws IOException {
        // Default implementation. Does nothing
    }


    @Override
    /**
     *  Just sum the values contained in the iterable, compute the average and wrap into a IntFloatCoupleWritable
     *  instance
     */
    public void reduce(Text key, Iterable<IntWritable> valuesIterable, Context context)
            throws IOException, InterruptedException {
        int n_weeks = context.getConfiguration().getInt("n_weeks", -1);
        Iterator<IntWritable> values = valuesIterable.iterator();
        if (n_weeks == -1) {
            throw new RuntimeException();
        }
        int sum = 0;
        while (values.hasNext()) {
            sum += values.next().get();
        }
        float average = ((float) sum) / n_weeks;
        context.write(key, new IntFloatCoupleWritable(sum, average));
    }

}
