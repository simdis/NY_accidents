package hadoop.ny_accidents.borough_per_week;


import hadoop.ny_accidents.types.BoroughWeekWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Get as input the output of BoroughPerWeekReducer and map into into <borough, numberOfLethalAccidents>
 *
 * @author fusiled
 */
public class AverageBoroughPerWeekMapper extends Mapper<BoroughWeekWritable, IntWritable, Text, IntWritable> {


    @Override
    public void cleanup(Context context) throws IOException {
        // Default implementation. Does nothing
    }

    @Override
    /**
     * The map function generates a token <borough, numberOfLethalAccidents> for each fatal accidents.
     */
    public void map(BoroughWeekWritable key, IntWritable nLethalAccidentsPerWeek, Context context)
            throws IOException, InterruptedException {
        context.write(key.getBorough(), nLethalAccidentsPerWeek);
    }


}
