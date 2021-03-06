package hadoop.ny_accidents.contributing_factors;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import hadoop.ny_accidents.types.SumAndAverageWritable;

/**
 * A class that defines the job used for retrieving the number of accidents per contributing factor
 * and the percentage of fatal accidents (per contributing factor) into the NYPD dataset.
 * 
 * Note that the way in which the contributing factors are handled is different according to which
 * vehicle is referred to. In the first column the empty or "Unspecified" factors are considered 
 * as "Unspecified" (they can be statistically relevant even if some of them may are due to a 
 * difficulty in understanding the cause of the accident whereas some others may are due to 
 * imprecisions in compiling the accident's report.). However, in the other 4 columns the 
 * "Unspecified" or empty fields are simply ignored since they are defined if and only if there 
 * are enough involved vehicles.
 * 
 * @author Simone Disabato
 *
 */
public class ContributingFactors extends Configured implements Tool{
     
    @Override
    public int run(String[] args) throws IOException {
        
        // Retrieve the configuration.
        Configuration conf = getConf();
        
        // Initialize the job
        JobConf job = new JobConf(conf, ContributingFactors.class);
        job.setJobName("ContributingFactor");
        
        // Define the output classes.
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(SumAndAverageWritable.class);
        
        // Define the mapper and the reducer.
        job.setMapperClass(ContributingFactorsMapper.class);
        job.setReducerClass(ContributingFactorsReducer.class);
        
        // Set the classes for the mapper and the reducer.
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(SumAndAverageWritable.class);
        
        job.setInputFormat(TextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);
        
        // Delete outputs folder if exists.
        File f = new File(new String(args[1]));
        if (f.exists() && f.isDirectory()) {
            // If the output directory already exists must be deleted.
            FileUtils.cleanDirectory(f); //clean out directory
            FileUtils.forceDelete(f);
        }
        
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        // Run the job.
        JobClient.runJob(job);

        return 0;
    }

    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ContributingFactors(), args);
        System.exit(res);
    }
}
