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
            Long logicalRecordPart = Long.parseLong(text.substring(24,28));
            Long totalParts =Long.parseLong(text.substring(28,32));

            AverageRoomRecord record = new AverageRoomRecord();

            if (logicalRecordPart.equals(totalParts)) {
                record.setRoomCounts(getRoomCounts(record, text));
                context.write(new Text(state), record);
            }
        }
    }

    private long[] getRoomCounts(AverageRoomRecord record, String unparsedText) {
        long[] roomSizes = new long[record.getNumberRooms()];

        int count = 0;
        for (int i = 2388; i < 2461; i += 9) {
            long number = Long.parseLong(unparsedText.substring(i, i + 9));
            roomSizes[count] = number;
            count++;
        }

        return roomSizes;
    }
}
