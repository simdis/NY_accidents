package hadoop.ny_accidents.contributing_factorsN;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
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
 * The version N is developed according to new API (org.hadoop.mapreduce).
 * 
 * @author Simone Disabato
 *
 */
public class ContributingFactorsN extends Configured implements Tool{
     
    @Override
    public int run(String[] args) throws IOException {
        
        // Retrieve the configuration.
        Configuration conf = getConf();
        
        // Initialize the job
        Job job = new Job(conf);
        job.setJobName("ContributingFactor");
        job.setJarByClass(ContributingFactorsN.class);
        
        // Define the output classes.
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(SumAndAverageWritable.class);
        
        // Define the mapper and the reducer.
        job.setMapperClass(ContributingFactorsMapperN.class);
        job.setReducerClass(ContributingFactorsReducerN.class);
        
        // Set the classes for the mapper and the reducer.
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(SumAndAverageWritable.class);
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
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
        try {
            job.waitForCompletion(true);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return 1;
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return 1;
        }
        return 0;
    }

    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ContributingFactorsN(), args);
        System.exit(res);
    }
}
