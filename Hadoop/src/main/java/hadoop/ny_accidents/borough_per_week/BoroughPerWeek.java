package hadoop.ny_accidents.borough_per_week;

import hadoop.ny_accidents.types.BoroughWeekWritable;
import hadoop.ny_accidents.types.IntFloatCoupleWritable;
import hadoop.ny_accidents.util.NumberOfWeekProducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Main class for the point 3 of the project. We want to compute the number of lethal accidents foreach week and
 * the average number of accidents per week foreach borough.
 */
public class BoroughPerWeek extends Configured implements Tool {

    private static String intermediate = "boroughWeekTemp";
    private static NumberOfWeekProducer nwp;

    public static void main(String[] args) throws Exception {
        nwp = new NumberOfWeekProducer(args[0]);
        System.out.println("n_weeks is: " + nwp.getNumberOfWeeks());
        int res = ToolRunner.run(new Configuration(), new BoroughPerWeek(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {


        Configuration conf = getConf();
        conf.setInt("n_weeks", nwp.getNumberOfWeeks());

        Job jobConf1 = new Job(conf);
        jobConf1.setJobName("Number of Accidents Per Borough Foreach Week");
        jobConf1.setJarByClass(BoroughPerWeek.class);

        Path in = new Path(args[0]);
        Path out = new Path(intermediate);
        FileInputFormat.setInputPaths(jobConf1, in);
        FileOutputFormat.setOutputPath(jobConf1, out);


        jobConf1.setMapperClass(BoroughPerWeekMapper.class);
        jobConf1.setReducerClass(BoroughPerWeekReducer.class);

        jobConf1.setInputFormatClass(TextInputFormat.class);
        jobConf1.setOutputFormatClass(SequenceFileOutputFormat.class);

        jobConf1.setOutputKeyClass(BoroughWeekWritable.class);
        jobConf1.setOutputValueClass(IntWritable.class);

        jobConf1.waitForCompletion(true);

        /**
         * JOB 2
         */
        Job jobConf2 = new Job(conf);
        jobConf2.setJarByClass(BoroughPerWeek.class);
        jobConf2.setJobName("Average Accidents Per Borough on weekly basis");

        Path in2 = new Path(intermediate); //temp
        Path out2 = new Path(args[1]); //output
        FileInputFormat.setInputPaths(jobConf2, in2);
        FileOutputFormat.setOutputPath(jobConf2, out2);

        jobConf2.setMapperClass(AverageBoroughPerWeekMapper.class);
        jobConf2.setReducerClass(AverageBoroughPerWeekReducer.class);

        jobConf2.setMapOutputKeyClass(Text.class);
        jobConf2.setMapOutputValueClass(IntWritable.class);

        jobConf2.setInputFormatClass(SequenceFileInputFormat.class);
        jobConf2.setOutputFormatClass(TextOutputFormat.class);

        jobConf2.setOutputKeyClass(Text.class);
        jobConf2.setOutputValueClass(IntFloatCoupleWritable.class);

        jobConf2.waitForCompletion(true);

        return 0;
    }
}
