package cs455.SocialJobs.NonMarried;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

import cs455.RecordUtil.NonMarriedRecord;

public class NonMarriedMapper extends Mapper<LongWritable, Text, Text, NonMarriedRecord> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String text = value.toString();
		String summary = text.substring(10,13);

		if(summary.equals("100")) {
			String state = text.substring(8,10);

			NonMarriedRecord record = new NonMarriedRecord();

			record.setNonMarriedMale(getNonMarriedMale(text));
			record.setNonMarriedFemale(getNonMarriedFemale(text));
			record.setPopulation(getPopulation(text));
			context.write(new Text(state), record);
			
		}
	}

	private Long getNonMarriedMale(String text) {
		return Long.parseLong(text.substring(4423,4432));
	}

	private Long getNonMarriedFemale(String text) {
		return Long.parseLong(text.substring(4467, 4476));
	}

	private Long getPopulation(String text) {
		return Long.parseLong(text.substring(300, 309));
	}
}
