package hadoop.ny_accidents.lethal_accidents;

import java.io.IOException;
import java.text.ParseException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import hadoop.ny_accidents.map_attributes.NYPDAttributes;
import hadoop.ny_accidents.map_attributes.NYPDAttributesMap;
import hadoop.ny_accidents.types.WeekYearWritable;
import hadoop.ny_accidents.util.DateUtility;
import hadoop.ny_accidents.util.StringParser;
import hadoop.ny_accidents.util.WeekYear;

/**
 * The Mapper for the LethalAccidents class.
 * @author Simone Disabato
 *
 */
public class LethalAccidentsMapper implements Mapper<LongWritable,Text,WeekYearWritable,IntWritable>{
    // Define the enumeration of attributes
    private static final NYPDAttributesMap attributes = new NYPDAttributesMap();
    private static final int fields = NYPDAttributes.values().length;
    private static final IntWritable one = new IntWritable(1);
    private StringParser parser = new StringParser();
    private DateUtility dateUtility = new DateUtility();

    @Override
    public void configure(JobConf conf) {
        // Default implementation. Does nothing
    }

    @Override
    public void close() throws IOException {
        // Default implementation. Does nothing
    }

    @Override
    /**
     * The map function generates a token <(year,week),1> for each fatal accidents.
     */
    public void map(LongWritable key, Text text, OutputCollector<WeekYearWritable, IntWritable> output, Reporter reporter)
            throws IOException {
        // Parse the string
        String[] tokens = parser.split(text.toString(), ',', '"');
        if (tokens.length != fields) {
            // The line has a wrong number of attributes, thus it is discarded
            return;
        }
        // Now the output token is defined.
        int personsKilled = Integer.parseInt(tokens[attributes.getKey(NYPDAttributes.NUMBER_OF_PERSONS_KILLED)]);
        // An accident is fatal iff there is at least a death.
        if (personsKilled > 0) {
            // Parse the date to retrieve the key
            WeekYear weekYear;
            try {
                weekYear = dateUtility.computeWeekYear(tokens[attributes.getKey(NYPDAttributes.DATE)]);
            } catch (ParseException e) {
                System.out.println("Error on parsing the date string: "+tokens[attributes.getKey(NYPDAttributes.DATE)]);
                e.printStackTrace();
                return;
            }
            
            // Generate the output token
            WeekYearWritable outKey = new WeekYearWritable(weekYear.getYear(),weekYear.getWeek());
            output.collect(outKey, one);
        }
        
        
    }

}
