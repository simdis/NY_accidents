package hadoop.ny_accidents.borough_per_week;

import hadoop.ny_accidents.types.BoroughWeekWritable;
import hadoop.ny_accidents.types.IntCoupleWritable;
import hadoop.ny_accidents.util.NumberOfWeekProducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
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
    private static String totalAccidentsOut = "totalAccidentsOut";
    private static NumberOfWeekProducer nwp;
    private static int LETHAL_ID = 1;
    private static int NO_LETHAL_ID = 2;

    public static void main(String[] args) throws Exception {
        nwp = new NumberOfWeekProducer(args[0]);
        System.out.println("n_weeks is: " + nwp.getNumberOfWeeks());
        System.out.println("average result will be saved at "+args[1]);
        System.out.println("number of accidents will be saved at "+totalAccidentsOut);
        int res = ToolRunner.run(new Configuration(), new BoroughPerWeek(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {


        Configuration conf = getConf();

        conf.setInt("lethal_id", LETHAL_ID);
        conf.setInt("no_lethal_id", NO_LETHAL_ID);
        conf.setInt("n_weeks", nwp.getNumberOfWeeks());

        Job baseConf = new Job(conf);
        baseConf.setJobName("Number of lethal and non lethal accidents Per Borough Foreach Week");
        baseConf.setJarByClass(BoroughPerWeek.class);

        Path in = new Path(args[0]);
        Path out = new Path(intermediate);
        FileInputFormat.setInputPaths(baseConf, in);
        FileOutputFormat.setOutputPath(baseConf, out);


        baseConf.setMapperClass(BaseBoroughPerWeekMapper.class);
        baseConf.setCombinerClass(BaseBoroughPerWeekReducer.class);
        baseConf.setReducerClass(BaseBoroughPerWeekReducer.class);

        baseConf.setInputFormatClass(TextInputFormat.class);
        baseConf.setOutputFormatClass(SequenceFileOutputFormat.class);

        baseConf.setMapOutputKeyClass(BoroughWeekWritable.class);
        baseConf.setMapOutputValueClass(IntWritable.class);

        baseConf.setOutputKeyClass(BoroughWeekWritable.class);
        baseConf.setOutputValueClass(IntCoupleWritable.class);

        baseConf.waitForCompletion(true);

        /**
         * JOB 2
         */
        Job averageLethalAccidentsConf = new Job(conf);
        averageLethalAccidentsConf.setJarByClass(BoroughPerWeek.class);
        averageLethalAccidentsConf.setJobName("Average Accidents Per Borough on weekly basis");

        Path in2 = new Path(intermediate); //temp
        Path out2 = new Path(args[1]); //output
        FileInputFormat.setInputPaths(averageLethalAccidentsConf, in2);
        FileOutputFormat.setOutputPath(averageLethalAccidentsConf, out2);

        averageLethalAccidentsConf.setMapperClass(AverageLethalBoroughPerWeekMapper.class);
        averageLethalAccidentsConf.setCombinerClass(AverageLethalBoroughPerWeekReducer.class);
        averageLethalAccidentsConf.setReducerClass(AverageLethalBoroughPerWeekReducer.class);

        averageLethalAccidentsConf.setMapOutputKeyClass(Text.class);
        averageLethalAccidentsConf.setMapOutputValueClass(IntWritable.class);

        averageLethalAccidentsConf.setInputFormatClass(SequenceFileInputFormat.class);
        averageLethalAccidentsConf.setOutputFormatClass(TextOutputFormat.class);

        averageLethalAccidentsConf.setOutputKeyClass(Text.class);
        averageLethalAccidentsConf.setOutputValueClass(FloatWritable.class);

        averageLethalAccidentsConf.waitForCompletion(true);


        /**
         * JOB 3
         */

        Job numberOfAccidentsPerBoruoughPerWeek = new Job(conf);
        numberOfAccidentsPerBoruoughPerWeek.setJarByClass(BoroughPerWeek.class);
        numberOfAccidentsPerBoruoughPerWeek.setJobName("Number of Accidents per borough foreach week");

        Path in3 = new Path(intermediate); //temp
        Path out3 = new Path(totalAccidentsOut); //output
        FileInputFormat.setInputPaths(numberOfAccidentsPerBoruoughPerWeek, in3);
        FileOutputFormat.setOutputPath(numberOfAccidentsPerBoruoughPerWeek, out3);

        numberOfAccidentsPerBoruoughPerWeek.setMapperClass(TotalBoroughPerWeekMapper.class);
        numberOfAccidentsPerBoruoughPerWeek.setCombinerClass(Reducer.class);
        numberOfAccidentsPerBoruoughPerWeek.setReducerClass(Reducer.class);

        numberOfAccidentsPerBoruoughPerWeek.setMapOutputKeyClass(BoroughWeekWritable.class);
        numberOfAccidentsPerBoruoughPerWeek.setMapOutputValueClass(IntWritable.class);

        numberOfAccidentsPerBoruoughPerWeek.setInputFormatClass(SequenceFileInputFormat.class);
        numberOfAccidentsPerBoruoughPerWeek.setOutputFormatClass(TextOutputFormat.class);

        numberOfAccidentsPerBoruoughPerWeek.setOutputKeyClass(BoroughWeekWritable.class);
        numberOfAccidentsPerBoruoughPerWeek.setOutputValueClass(IntWritable.class);

        numberOfAccidentsPerBoruoughPerWeek.waitForCompletion(true);

        /**
         * JOB 4
         */

        Job intIdentity = new Job(conf);
        intIdentity.setJarByClass(BoroughPerWeek.class);
        intIdentity.setJobName("Intermediate Identity");

        Path in4 = new Path(intermediate); //temp
        Path out4 = new Path("intIdentity"); //output
        FileInputFormat.setInputPaths(intIdentity, in4);
        FileOutputFormat.setOutputPath(intIdentity, out4);

        intIdentity.setMapperClass(Mapper.class);
        intIdentity.setCombinerClass(Reducer.class);
        intIdentity.setReducerClass(Reducer.class);

        intIdentity.setMapOutputKeyClass(BoroughWeekWritable.class);
        intIdentity.setMapOutputValueClass(IntCoupleWritable.class);

        intIdentity.setInputFormatClass(SequenceFileInputFormat.class);
        intIdentity.setOutputFormatClass(TextOutputFormat.class);

        intIdentity.setOutputKeyClass(BoroughWeekWritable.class);
        intIdentity.setOutputValueClass(IntCoupleWritable.class);

        intIdentity.waitForCompletion(true);

        return 0;
    }
}
