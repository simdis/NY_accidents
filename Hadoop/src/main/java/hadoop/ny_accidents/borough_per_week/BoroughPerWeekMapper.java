package hadoop.ny_accidents.borough_per_week;

import hadoop.ny_accidents.map_attributes.NYPDAttributes;
import hadoop.ny_accidents.types.BoroughWeekWritable;
import hadoop.ny_accidents.util.StringParser;
import hadoop.ny_accidents.types.WeekYear;
import hadoop.ny_accidents.util.WeekYearBuilder;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;

/**
 * We want to count the number of lethal accidents foreach (borough, weekYear), so
 * from input we produce <(borough, weekYear), ONE >
 *
 * @author fusiled
 *         Modified from Simone Disabato point 1 mapper
 */
public class BoroughPerWeekMapper extends Mapper<Object, Text, BoroughWeekWritable, IntWritable> {
    // Define the enumeration of attributes
    private static final int fields = NYPDAttributes.values().length;
    private static final IntWritable one = new IntWritable(1);
    private static final String BOROUGH_UNKNOWN_NAME = "UNKNOWN";
    private StringParser parser = new StringParser();


    @Override
    public void cleanup(Context context) throws IOException {
        // Default implementation. Does nothing
    }

    /**
     * The map function generates a token <(borough,(year,week) ),one> for each fatal accidents.
     */
    @Override
    public void map(Object key, Text text, Context context)
            throws IOException, InterruptedException {
        // Parse the string
        String[] tokens = parser.split(text.toString(), ',', '"');
        if (tokens.length != fields) {
            // The line has a wrong number of attributes, thus it is discarded
            return;
        }
        int personsKilled;
        String personsKilledString = "";
        try {
            // Now the output token is defined.
            personsKilledString = tokens[NYPDAttributes.NUMBER_OF_PERSONS_KILLED.get()];
            personsKilled = Integer.parseInt(personsKilledString);
        } catch (NumberFormatException nfe) {
            System.out.println("Number Format exception for " + personsKilledString);
            return;
        }
        // An accident is fatal iff there is at least a death.
        if (personsKilled > 0) {
            // Parse the date to retrieve the key
            WeekYear weekYear;
            try {
                weekYear = new WeekYearBuilder().build(tokens[NYPDAttributes.DATE.get()]);
            } catch (ParseException e) {
                System.out.println("Error on parsing the date string: " + tokens[NYPDAttributes.DATE.get()]);
                e.printStackTrace();
                return;
            }
            //The name of the borough is not present, so we call it UNKNOWN
            String borough = tokens[NYPDAttributes.BOROUGH.get()];
            if (borough.equals("") || borough.equals(" ")) {
                borough = BOROUGH_UNKNOWN_NAME;
            }
            // Generate the output token
            BoroughWeekWritable bwOutKey = new BoroughWeekWritable(borough, weekYear);
            context.write(bwOutKey, one);
        }

    }

}
