package cs455.HousingJobs.AverageRooms;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cs455.RecordUtil.AverageRoomRecord;

public class AverageRoomMapper  extends Mapper<LongWritable, Text, Text, AverageRoomRecord> {

    @Override
    protected void map(LongWritable key, Text value, Context context)  throws IOException, InterruptedException {

        String text = value.toString();
        String summary =text.substring(10,13);
        if (summary.equals("100")) {

            String state = text.substring(8,10);

            AverageRoomRecord record = new AverageRoomRecord();

			record.setRoomCounts(getRoomCounts(record, text));
			context.write(new Text(state), record);
        }
    }

	// Gathers information from the text file
    private long[] getRoomCounts(AverageRoomRecord record, String text) {
        long[] roomSizes = new long[record.getNumberRooms()];

        int room = 0;
        for (int index = 2388; index < 2461; index += 9) {
            roomSizes[room] = Long.parseLong(text.substring(index, index + 9));
            room++;
        }

        return roomSizes;
    }
}
