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

		String text = value.toString();
		String summary = text.substring(10,13);

		if(summary.equals("100")) {
			String state = text.substring(8,10);

			HousingRecord housingRecord = new HousingRecord();

			housingRecord.setRural(Long.parseLong(text.substring(1839,1848)));
			housingRecord.setUrban(Long.parseLong(text.substring(1821,1830)) + Long.parseLong(text.substring(1830,1839)));
			context.write(new Text(state), housingRecord);
		}
	}
}
