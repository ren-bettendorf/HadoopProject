package cs455.HousingJobs.RentedOwned;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cs455.RecordUtil.RentedOwnedRecord;

public class RentedOwnedMapper extends Mapper<LongWritable, Text, Text, RentedOwnedRecord> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String text = value.toString();
		String summary = text.substring(10,13);

		if(summary.equals("100")) {
			String state = text.substring(8,10);
			RentedOwnedRecord record = new RentedOwnedRecord();

			record.setRented(Long.parseLong(text.substring(1812, 1821));
			record.setOwned(Long.parseLong(text.substring(1803, 1812)));
			context.write(new Text(state), record);
		}
	}
}
