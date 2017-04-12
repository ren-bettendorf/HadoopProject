package cs455.SocialJobs.NonMarried;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

import cs455.RecordUtil.NonMarriedRecord;

public class NonMarriedJob {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "NonMarriedJob");
            
            job.setJarByClass(NonMarriedJob.class);
            job.setMapperClass(NonMarriedMapper.class);
            job.setCombinerClass(NonMarriedReducer.class);
            job.setReducerClass(NonMarriedReducer.class);
            
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(NonMarriedRecord.class);
            
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NonMarriedRecord.class);
            
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (IOException e) {
            System.err.println(e.getMessage());
        } catch (InterruptedException e) {
            System.err.println(e.getMessage());
        } catch (ClassNotFoundException e) {
            System.err.println(e.getMessage());
        }

    }
}
