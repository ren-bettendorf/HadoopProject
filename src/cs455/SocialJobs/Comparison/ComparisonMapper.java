package cs455.SocialJobs.Comparison;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cs455.RecordUtil.ComparisonRecord;

public class ComparisonMapper  extends Mapper<LongWritable, Text, Text, ComparisonRecord> {

    @Override
    protected void map(LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
		
        String text = value.toString();
        String summary =text.substring(10,13);
        if (summary.equals("100")) {

            String state = text.substring(8,10);

            ComparisonRecord record = new ComparisonRecord();

			record.setRoomCounts(getRoomCounts(record, text));
			record.setUrbanPopulation(Long.parseLong(text.substring(327,336)));
			record.setTotalPopulation(Long.parseLong(text.substring(327,336)) + Long.parseLong(text.substring(336,345)));
			context.write(new Text(state), record);
        }
    }

    private long[] getRoomCounts(ComparisonRecord record, String text) {
        long[] roomSizes = new long[record.getNumberRooms()];

        int room = 0;
        for (int index = 2388; index < 2461; index += 9) {
            roomSizes[room] = Long.parseLong(text.substring(index, index + 9));
            room++;
        }

        return roomSizes;
    }
}
