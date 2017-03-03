package hadoop.ny_accidents.contributing_factorsN;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import hadoop.ny_accidents.map_attributes.NYPDAttributes;
import hadoop.ny_accidents.map_attributes.NYPDAttributesMap;
import hadoop.ny_accidents.util.StringParser;

/**
 * Mapper class for the contributing factors job.
 * The version N is developed according to new API (org.hadoop.mapreduce).
 * 
 * @author Simone Disabato
 *
 */
public class ContributingFactorsMapperN extends Mapper<LongWritable,Text,Text,IntWritable>{

    private static final NYPDAttributesMap attributes = new NYPDAttributesMap();
    private static final int fields = NYPDAttributes.values().length;
    private StringParser parser = new StringParser();
    private static final int contributingFactors = 5;
    private static final int contributingFactorDescriptionLength = 60;
    
    @Override
    public void cleanup(Context context) throws IOException {
        // Default implementation. Does nothing
    }

    @Override
    /**
     * The map function generates a token <contributing factor,#deaths> for each accident.
     */
    public void map(LongWritable key, Text text, Context context)
            throws IOException, InterruptedException {
        // Parse the string
        String[] tokens = parser.split(text.toString(), ',', '"');
        if (tokens.length != fields) {
            // The line has a wrong number of attributes, thus it is discarded
            return;
        }
        // The key is the contributing factor of the vehicle 1.
        String keyString = tokens[attributes.getKey(NYPDAttributes.CONTRIBUTING_FACTOR_VEHICLE_1)];
        // For completeness the rows that do not have the contributing factor of the vehicle 1
        // are mapped as "Unspecified".
        if (keyString.isEmpty()) {
            keyString = "Unspecified";
        }
        // Convert the key to a fixed length
        keyString = StringUtils.rightPad(keyString, contributingFactorDescriptionLength);
        int deaths = Integer.parseInt(tokens[attributes.getKey(NYPDAttributes.NUMBER_OF_PERSONS_KILLED)]);
        context.write(new Text(keyString), new IntWritable(deaths));
        // Now, we check for the other columns' contributing factors.
        // Note that we exploit the fact that the contributing factor's columns are one near
        // the other.
        for (int i=1; i < contributingFactors;i++) {
            keyString = tokens[attributes.getKey(NYPDAttributes.CONTRIBUTING_FACTOR_VEHICLE_1)+i];
            if (!keyString.isEmpty() && !keyString.equals("Unspecified")) {
                // Convert the key to a fixed length
                keyString = StringUtils.rightPad(keyString, contributingFactorDescriptionLength);
                context.write(new Text(keyString), new IntWritable(deaths));
            }
        }
        
    }
}
