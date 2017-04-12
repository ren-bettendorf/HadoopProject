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

            if (logicalRecordPart.equals(totalParts)) {
                record.setMap(getHousingValues(record, text));
                context.write(new Text(state), record);
            }
        }
    }

    private Map<String, Long> getHousingValues(MedianValueRecord record, String text) {
        Map<String, Long> stringLongMap = new LinkedHashMap<String, Long>();

        int count = 0;
        for (int i = 2928; i < 3100; i += 9) {
            long number = Long.parseLong(text.substring(i, i + 9));
            stringLongMap.put(record.getValueList().get(count), number);
            count++;
        }

        return stringLongMap;
    }
}
