package hadoop.ny_accidents.lethal_accidents;

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

import hadoop.ny_accidents.types.WeekYearWritable;

/**
 * A class that defines the job used for retrieving the number of fatal accidents per week
 * into the NYPD dataset.
 * @author Simone Disabato
 *
 */
public class LethalAccidents extends Configured implements Tool {
    
    @Override
    public int run(String[] args) throws IOException {
        
        // Retrieve the configuration.
        Configuration conf = getConf();
        
        // Initialize the job
        JobConf job = new JobConf(conf, LethalAccidents.class);
        job.setJobName("lethalAccidents");
        
        // Define the output classes.
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        // Define the mapper and the reducer.
        job.setMapperClass(LethalAccidentsMapper.class);
        job.setReducerClass(LethalAccidentsReducer.class);
        
        // Set the classes for the mapper and the reducer.
        job.setMapOutputKeyClass(WeekYearWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(WeekYearWritable.class);
        job.setOutputValueClass(IntWritable.class);
        
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
        int res = ToolRunner.run(new Configuration(), new LethalAccidents(), args);
        System.exit(res);
    }
}
