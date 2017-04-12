package cs455.HousingJobs.AverageRooms;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import cs455.RecordUtil.AverageRoomRecord;

/**
 * This combiner is used to combine all average room sizes per state during Map/Reduce so the reducer
 * can reduce the data to one entry for the US.
 */
public class AverageRoomCombiner  extends Reducer<Text, AverageRoomRecord, Text, AverageRoomRecord> {
    @Override
    protected void reduce(Text key, Iterable<AverageRoomRecord> values, Context context)  throws IOException, InterruptedException {
        AverageRoomRecord record = new AverageRoomRecord();

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
