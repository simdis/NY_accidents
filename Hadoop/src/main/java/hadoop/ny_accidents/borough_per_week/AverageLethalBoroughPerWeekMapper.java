package hadoop.ny_accidents.borough_per_week;


import hadoop.ny_accidents.types.BoroughWeekWritable;
import hadoop.ny_accidents.types.IntCoupleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Get as input the output of BasicBoroughPerWeekReducer and map into into <borough, numberOfLethalAccidents>
 *
 * @author fusiled <fusiled@gmail.com>
 */
public class AverageLethalBoroughPerWeekMapper extends Mapper<BoroughWeekWritable, IntCoupleWritable, Text, IntWritable> {


    @Override
    public void cleanup(Context context) throws IOException {
        // Default implementation. Does nothing
    }

    @Override
    /**
     * The map function generates a token <borough, numberOfLethalAccidents> for each fatal accidents.
     */
    public void map(BoroughWeekWritable key, IntCoupleWritable nLethalAccidentsPerWeek, Context context)
            throws IOException, InterruptedException {
        context.write(key.getBorough(), nLethalAccidentsPerWeek.getSecond());
    }


}
