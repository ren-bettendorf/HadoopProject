package cs455.SocialJobs.Comparison;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cs455.RecordUtil.ComparisonRecord;

public class ComparisonJob {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "AverageRoom");
            job.setJarByClass(ComparisonJob.class);
            job.setMapperClass(ComparisonMapper.class);
            job.setCombinerClass(ComparisonReducer.class);
            job.setNumReduceTasks(1);
            job.setReducerClass(ComparisonReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(ComparisonRecord.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(ComparisonRecord.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            System.err.println(e.getMessage());
        }
    }
}
