package cs455.SocialJobs.Comparison;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import cs455.RecordUtil.ComparisonRecord;

public class ComparisonReducer extends Reducer<Text, ComparisonRecord, Text, ComparisonRecord> {

    private long percent;

    @Override
    protected void reduce(Text key, Iterable<ComparisonRecord> values, Context context)  throws IOException, InterruptedException {
		long averageRoom = 0L;
		long urban = 0L;
		long total = 0L;
		
        for (ComparisonRecord val : values) {
			long totalHouses = 0L;
			long totalRooms = 0L;
			long[] roomCounts = val.getRoomCounts();
			for (int room = 0; room < roomCounts.length; room++) {
				totalHouses += roomCounts[room];
				totalRooms += roomCounts[room] * (room + 1);	
			}
			averageRoom += totalRooms / totalHouses;
			urban += val.getUrbanPopulation();
			total += val.getTotalPopulation();
        }
		ComparisonRecord record = new ComparisonRecord();
		record.setAverageRoom(averageRoom);
		record.setUrbanPopulation(urban);
		record.setTotalPopulation(total);
		context.write(key, record);
    }

    private long computeAverageRoom(long[] roomCounts) {
        long totalHouses = 0;
        long totalRooms = 0;
        for (int room = 0; room < roomCounts.length; room++) {
            totalHouses += roomCounts[room];
            for (int count = 0; count < roomCounts[room]; count++) {
                totalRooms += room + 1;
            }
        }
        return totalRooms / totalHouses;
    }
}
