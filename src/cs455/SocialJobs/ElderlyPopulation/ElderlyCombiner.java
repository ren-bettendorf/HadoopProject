package cs455.SocialJobs.ElderlyPopulation;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import cs455.RecordUtil.ElderlyRecord;

public class ElderlyCombiner extends Reducer<Text, ElderlyRecord, Text, ElderlyRecord> {
    @Override
    protected void reduce(Text key, Iterable<ElderlyRecord> values, Context context) throws IOException, InterruptedException {
        long totalPopulation = 0;
        long elderlyPopulation = 0;

        for (ElderlyRecord val : values) {
            totalPopulation += val.getTotalPopulation();
            elderlyPopulation += val.getElderlyPopulation();
        }

        ElderlyRecord record = new ElderlyRecord();
		record.setTotalPopulation(totalPopulation);
		record.setElderlyPopulation(elderlyPopulation);
        context.write(key, record);
    }
}
