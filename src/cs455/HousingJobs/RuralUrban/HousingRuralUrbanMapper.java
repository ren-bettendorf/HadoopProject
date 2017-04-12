package cs455.HousingJobs.RuralUrban;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

import cs455.RecordUtil.HousingRecord;

/**
 * Mapper: Reads line by line, split them into words. Emit <word, 1> pairs.
 */
public class HousingRuralUrbanMapper extends Mapper<LongWritable, Text, Text, HousingRecord> {

    	@Override
    	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        	String unparsedText = value.toString();
		String summary = unparsedText.substring(10,13);

		if(summary.equals("100")) {
			String state = unparsedText.substring(8,10);
			Long logicalRecordPart = Long.parseLong(unparsedText.substring(24,28));
			Long totalParts = Long.parseLong(unparsedText.substring(28,32));

			HousingRecord housingRecord = new HousingRecord();

			if(logicalRecordPart.equals(totalParts)) {
				housingRecord.setRural(getRural(unparsedText));
				housingRecord.setUrban(getUrban(unparsedText));
				housingRecord.setLogicalRecordPart(logicalRecordPart);
				housingRecord.setTotalParts(totalParts);
				context.write(new Text(state), housingRecord);
			}
		}
	}

	private Long getRural(String text) {
		return Long.parseLong(text.substring(1839,1848));
	}

	private Long getUrban(String text) {
		return Long.parseLong(text.substring(1821,1830)) + Long.parseLong(text.substring(1830,1839));
	}
}
