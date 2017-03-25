package hadoop.ny_accidents.borough_per_week;

import hadoop.ny_accidents.types.BoroughWeekWritable;
import hadoop.ny_accidents.types.IntCoupleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 *
 */
public class BaseBoroughPerWeekReducer extends Reducer<BoroughWeekWritable, IntWritable, BoroughWeekWritable, IntCoupleWritable> {

    @Override
    public void cleanup(Context context) throws IOException {
        // Default implementation. Does nothing
    }

    /**
     * Produce the number of nonLethal and lethal accidents per borough foreach week
     */
    @Override
    public void reduce(BoroughWeekWritable key, Iterable<IntWritable> valuesIterable, Context context)
            throws InterruptedException, IOException {
        int lethal = context.getConfiguration().getInt("lethal_id",1);
        Iterator<IntWritable> values = valuesIterable.iterator();
        int lethalSum = 0;
        int total = 0;
        while (values.hasNext()) {
            int current = values.next().get();
            if(current==lethal)
            {
                lethalSum++;
            }
            total++;
        }
        context.write(key, new IntCoupleWritable(total,lethalSum));
    }

}
