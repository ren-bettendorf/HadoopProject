package cs455.HousingJobs.AverageRooms;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import cs455.RecordUtil.AverageRoomRecord;


public class AverageRoomCombiner  extends Reducer<Text, AverageRoomRecord, Text, AverageRoomRecord> {
    
	@Override
    protected void reduce(Text key, Iterable<AverageRoomRecord> values, Context context)  throws IOException, InterruptedException {
        AverageRoomRecord record = new AverageRoomRecord();
		
		// Combine into one record for the roomcounts
        for (AverageRoomRecord val : values) {
			long[] numberRooms = record.getRoomCounts();
			for (int i = 0; i < record.getNumberRooms(); i++) {
				numberRooms[i] += val.getRoomCounts()[i];
			}
			record.setRoomCounts(numberRooms);
        }

        context.write(key, record);
    }
}
