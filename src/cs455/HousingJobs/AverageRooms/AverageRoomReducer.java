package cs455.HousingJobs.AverageRooms;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import cs455.RecordUtil.AverageRoomRecord;

public class AverageRoomReducer extends Reducer<Text, AverageRoomRecord, Text, Text> {

    private long percent;

    @Override
    protected void reduce(Text key, Iterable<AverageRoomRecord> values, Context context)  throws IOException, InterruptedException {
        List<Long> averages = new ArrayList<Long>();

        for (AverageRoomRecord val : values) {
            averages.add(getAverage(val.getRoomCounts()));
        }

        Collections.sort(averages);

        percent = find95thPercentile(averages);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(new Text("95th Percentile"), new Text(percent + " rooms"));
    }

    public long getAverage(long[] roomCounts) {
        long totalHouses = 0;
        long totalRooms = 0;
        for (int i = 0; i < roomCounts.length; i++) {
            long numberOfHousesWithSize = roomCounts[i];
            totalHouses += numberOfHousesWithSize;
            for (int c = 0; c < numberOfHousesWithSize; c++) {
                totalRooms += i + 1;
            }
        }
        return totalRooms / totalHouses;
    }

    public long find95thPercentile(List<Long> averages) {
        int index =(int) 0.95*averages.size();
	return averages.get(index);
    }
}
