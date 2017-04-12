package cs455.HousingJobs.MedianValue;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cs455.RecordUtil.MedianValueRecord;

public class HouseMedianValueMapper extends Mapper<LongWritable, Text, Text, MedianValueRecord> {

    @Override
    protected void map(LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
		MedianValueRecord record = new MedianValueRecord();
		String text = value.toString();
		String summary = text.substring(10,13);

		if(summary.equals("100")) {
			String state = text.substring(8,10);

			Long logicalRecordPart = Long.parseLong(text.substring(24,28));
			Long totalParts = Long.parseLong(text.substring(28,32));
			
			if(logicalRecordPart.equals(totalParts)) {
				record.setMap(getHousingValues(record, text));
				context.write(new Text(state), record);
			}
        }
    }

	// Gathers information from the text file
    private Map<String, Long> getHousingValues(MedianValueRecord record, String text) {
        Map<String, Long> houseValueMap = new LinkedHashMap<String, Long>();

        int house = 0;
        for (int index = 2928; index < 3100; index += 9) {
            houseValueMap.put(record.getValueList().get(house), Long.parseLong(text.substring(index, index + 9)));
            house++;
        }

        return houseValueMap;
    }
}
