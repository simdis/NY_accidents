package hadoop.ny_accidents.borough_per_week;

import hadoop.ny_accidents.types.BoroughWeekWritable;
import hadoop.ny_accidents.types.IntCoupleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * We want to count the number of accidents foreach (borough, weekYear), so
 * from input we produce <(borough, weekYear), ONE >
 *
 * @author fusiled <fusiled@gmail.com>
 *         Modified from Simone Disabato point 1 mapper
 */
public class TotalBoroughPerWeekMapper extends Mapper<BoroughWeekWritable, IntCoupleWritable, BoroughWeekWritable, IntWritable> {

    @Override
    public void cleanup(Context context) throws IOException {
        // Default implementation. Does nothing
    }

    /**
     * The map function generates a token <(borough,(year,week) ),one> for each fatal accidents.
     */
    @Override
    public void map(BoroughWeekWritable key, IntCoupleWritable couple, Context context)
            throws IOException, InterruptedException {
        int total_accidents = couple.getFirst().get();
        context.write(key, new IntWritable(total_accidents)  );

    }

}
