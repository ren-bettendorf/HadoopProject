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

        for (MedianValueRecord val : values) {
            combineMap(record, val.getMap());
        }
        context.write(key, record);
    }

    private void combineMap(MedianValueRecord record, Map<String, Long> map) {
        Map<String, Long> fullMap = record.getMap();
        for (Map.Entry<String, Long> entry : map.entrySet()) {
            long value = fullMap.get(entry.getKey());
            value += entry.getValue();
            fullMap.put(entry.getKey(), value);
        }

        record.setMap(fullMap);
    }
}
