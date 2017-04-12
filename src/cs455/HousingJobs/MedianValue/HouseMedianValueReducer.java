package cs455.HousingJobs.MedianValue;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import cs455.RecordUtil.MedianValueRecord;

public class HouseMedianValueReducer  extends Reducer<Text, MedianValueRecord, Text, MedianValueRecord> {
    @Override
    protected void reduce(Text key, Iterable<MedianValueRecord> values, Context context) throws IOException, InterruptedException {
        MedianValueRecord record = new MedianValueRecord();

		// Computes the MedianValue by iterating through the VALUE_LIST until the median value is reached.
        for (MedianValueRecord val : values) {
            Map<String, Long> totalHouseMap = record.getMap();
			for (Map.Entry<String, Long> entry : val.getMap().entrySet()) {
				long value = totalHouseMap.get(entry.getKey());
				value += entry.getValue();
				totalHouseMap.put(entry.getKey(), value);
			}
			record.setMap(totalHouseMap);
        }
        context.write(key, record);
    }
}
