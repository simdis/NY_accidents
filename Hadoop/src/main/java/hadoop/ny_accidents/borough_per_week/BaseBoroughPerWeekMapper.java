package hadoop.ny_accidents.borough_per_week;

import hadoop.ny_accidents.map_attributes.NYPDAttributes;
import hadoop.ny_accidents.map_attributes.NYPDAttributesMap;
import hadoop.ny_accidents.types.BoroughWeekWritable;
import hadoop.ny_accidents.types.WeekYear;
import hadoop.ny_accidents.util.DateUtility;
import hadoop.ny_accidents.util.StringParser;
import hadoop.ny_accidents.util.WeekYearBuilder;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;

/**
 * We want to count the number of accidents foreach (borough, weekYear), so
 * from input we produce <(borough, weekYear), ONE >
 *
 * @author fusiled
 *         Modified from Simone Disabato point 1 mapper
 */
public class BaseBoroughPerWeekMapper extends Mapper<Object, Text, BoroughWeekWritable, IntWritable> {
    // Define the enumeration of attributes
    private static final int fields = NYPDAttributes.values().length;
    private static final String BOROUGH_UNKNOWN_NAME = "UNKNOWN";


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
        String[] tokens = new StringParser().split(text.toString(), ',', '"');
        if (tokens.length < fields) {
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

        // Parse the date to retrieve the key
        WeekYear weekYear;
        try {
            weekYear = new DateUtility().computeWeekYear(tokens[NYPDAttributes.DATE.get()]);
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
        int lethal = context.getConfiguration().getInt("lethal_id",1);
        int noLethal = context.getConfiguration().getInt("no_lethal_id", 2);
        // Generate the output token
        BoroughWeekWritable bwOutKey = new BoroughWeekWritable(borough, weekYear);
        int accidentType = personsKilled > 0 ? lethal : noLethal;
        context.write(bwOutKey, new IntWritable(accidentType) );

    }

}
