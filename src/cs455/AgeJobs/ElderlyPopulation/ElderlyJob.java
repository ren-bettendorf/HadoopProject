package cs455.AgeJobs.ElderlyPopulation;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cs455.RecordUtil.ElderlyRecord;

public class ElderlyJob {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "ElderlyPopulation");
            job.setJarByClass(ElderlyJob.class);
            job.setMapperClass(ElderlyMapper.class);
            job.setCombinerClass(ElderlyCombiner.class);
            job.setReducerClass(ElderlyReducer.class);
            job.setNumReduceTasks(1);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(ElderlyRecord.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            System.err.println(e.getMessage());
        }
    }
}
