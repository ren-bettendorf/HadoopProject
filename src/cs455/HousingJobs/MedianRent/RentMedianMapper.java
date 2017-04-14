package cs455.HousingJobs.MedianRent;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cs455.RecordUtil.MedianRentRecord;

public class RentMedianMapper extends Mapper<LongWritable, Text, Text, MedianRentRecord> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String text = value.toString();
        String summaryLevel = text.substring(10,13);
        if (summaryLevel.equals("100")) {

            String state = text.substring(8,10);
			
			Long logicalRecordPart = Long.parseLong(text.substring(24,28));
			Long totalParts = Long.parseLong(text.substring(28,32));
            MedianRentRecord record = new MedianRentRecord();
			
			if(logicalRecordPart.equals(totalParts)) {
				record.setMap(getRentValues(record, text));
				context.write(new Text(state), record);
			}
        }
    }

	// Gathers information from the text file
    private Map<String, Long> getRentValues(MedianRentRecord record, String text) {
        Map<String, Long> rentMap = new LinkedHashMap<>();

        int count = 0;
        for (int index = 3450; index < 3586; index += 9) {
            rentMap.put(record.getRentList().get(count), Long.parseLong(text.substring(index, index + 9)));
            count++;
        }

        return rentMap;
    }
}
