package cs455.HousingJobs.MedianRent;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import cs455.RecordUtil.MedianRentRecord;

public class RentMedianReducer extends Reducer<Text, MedianRentRecord, Text, MedianRentRecord> {
    @Override
    protected void reduce(Text key, Iterable<MedianRentRecord> values, Context context) throws IOException, InterruptedException {
      	MedianRentRecord record = new MedianRentRecord();

		// Computes the MedianRent by iterating through the RENT_LIST until the median value is reached.
        for (MedianRentRecord val : values) {
			Map<String, Long> totalRentMap = record.getMap();
			for (Map.Entry<String, Long> entry : val.getMap().entrySet()) {
				long currentValue = totalRentMap.get(entry.getKey());
				currentValue += entry.getValue();
				totalRentMap.put(entry.getKey(), currentValue);
			}
			record.setMap(totalRentMap);
        }
        context.write(key, record);
    }
}
