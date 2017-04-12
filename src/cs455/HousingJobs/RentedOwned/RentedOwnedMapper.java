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
			Long logicalRecordPart = Long.parseLong(text.substring(24,28));
			Long totalParts = Long.parseLong(text.substring(28,32));
			RentedOwnedRecord record = new RentedOwnedRecord();

			if (logicalRecordPart.equals(totalParts)) {
				record.setRented(getRenterOccupied(text));
                		record.setOwned(getOwnerOccupied(text));
                		context.write(new Text(state), record);
            		}
		}
	}

	private Long getOwnerOccupied(String unparsedText) {
        	Long ownerOccupied = Long.parseLong(unparsedText.substring(1803, 1812));
        	return ownerOccupied;
	}

	private Long getRenterOccupied(String unparsedText) {
        	Long renterOccupied = Long.parseLong(unparsedText.substring(1812, 1821));
        	return renterOccupied;
	}
}
