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

        for (MedianRentRecord val : values) {
            combineMap(record, val.getMap());
        }
        context.write(key, record);
    }

    private void combineMap(MedianRentRecord record, Map<String, Long> map) {
        Map<String, Long> fullMap = record.getMap();
        for (Map.Entry<String, Long> entry : map.entrySet()) {
            long value = fullMap.get(entry.getKey());
            value += entry.getValue();
            fullMap.put(entry.getKey(), value);
        }

        record.setMap(fullMap);
    }
}
