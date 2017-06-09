package hadoop.ny_accidents.borough_per_week;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Reducer class. It computes the total number of lethal accidents foreach borough and then the average on the number of weeks
 * is produced as output.
 * The number of weeks is obtained by Configuration attributes
 *
 * @author fusiled <fusiled@gmail.com>
 */
public class AverageLethalBoroughPerWeekReducer extends Reducer<Text, IntWritable, Text, FloatWritable> {

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
        float sum = 0;
        while (values.hasNext()) {
            sum += values.next().get();
        }
        float average =  sum / n_weeks;
        context.write(key, new FloatWritable(average));
    }

}
