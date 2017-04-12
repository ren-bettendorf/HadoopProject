package cs455.SocialJobs.NonMarried;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

import cs455.RecordUtil.NonMarriedRecord;

/**
 * Mapper: Reads line by line, split them into words. Emit <word, 1> pairs.
 */
public class NonMarriedMapper extends Mapper<LongWritable, Text, Text, NonMarriedRecord> {

    	@Override
    	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        	String unparsedText = value.toString();
		String summary = unparsedText.substring(10,13);

		if(summary.equals("100")) {
			String state = unparsedText.substring(8,10);
			Long logicalRecordPart = Long.parseLong(unparsedText.substring(24,28));
			Long totalParts = Long.parseLong(unparsedText.substring(28,32));

			NonMarriedRecord record = new NonMarriedRecord();

			if(logicalRecordPart.equals(totalParts)) {
				record.setNonMarriedMale(getNonMarriedMale(unparsedText));
				record.setNonMarriedFemale(getNonMarriedFemale(unparsedText));
				record.setPopulation(getPopulation(unparsedText));
				record.setLogicalRecordPart(logicalRecordPart);
				record.setTotalParts(totalParts);
				context.write(new Text(state), record);
			}
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
