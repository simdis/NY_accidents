package hadoop.ny_accidents.lethal_accidentsN;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import hadoop.ny_accidents.types.WeekYearWritable;

/**
 * A class that defines the job used for retrieving the number of fatal accidents per week
 * into the NYPD dataset.
 * The version N is developed according to new API (org.hadoop.mapreduce).
 * @author Simone Disabato
 *
 */
public class LethalAccidentsN extends Configured implements Tool {
    
    @Override
    public int run(String[] args) throws IOException {
        
        // Retrieve the configuration.
        Configuration conf = getConf();
        
        // Initialize the job
        Job job = new Job(conf);
        job.setJobName("lethalAccidents");
        job.setJarByClass(LethalAccidentsN.class);
        
        // Define the output classes.
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        // Define the mapper and the reducer. TODO
        job.setMapperClass(LethalAccidentsMapperN.class);
        job.setReducerClass(LethalAccidentsReducerN.class);
        
        // Set the classes for the mapper and the reducer.
        job.setMapOutputKeyClass(WeekYearWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(WeekYearWritable.class);
        job.setOutputValueClass(IntWritable.class);
        
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
        int res = ToolRunner.run(new Configuration(), new LethalAccidentsN(), args);
        System.exit(res);
    }
}
