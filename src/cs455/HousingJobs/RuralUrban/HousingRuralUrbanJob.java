package cs455.HousingJobs.RuralUrban;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

import cs455.RecordUtil.HousingRecord;

public class HousingRuralUrbanJob {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "HousingRuralUrban");
            
            job.setJarByClass(HousingRuralUrbanJob.class);
            job.setMapperClass(HousingRuralUrbanMapper.class);
            job.setCombinerClass(HousingRuralUrbanReducer.class);
            job.setReducerClass(HousingRuralUrbanReducer.class);
			
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(HousingRecord.class);
			
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(HousingRecord.class);
			
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
