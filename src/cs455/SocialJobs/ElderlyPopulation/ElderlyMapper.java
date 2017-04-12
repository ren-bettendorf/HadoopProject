package cs455.SocialJobs.ElderlyPopulation;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cs455.RecordUtil.ElderlyRecord;

public class ElderlyMapper extends Mapper<LongWritable, Text, Text, ElderlyRecord> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String text = value.toString();
        String summary = text.substring(10,13);
        if (summary.equals("100")) {

            String state = text.substring(8,10);
            Long logicalRecordPart = Long.parseLong(text.substring(24,28));
            Long totalParts = Long.parseLong(text.substring(28,32));


            if (!logicalRecordPart.equals(totalParts)) {
				ElderlyRecord record = new ElderlyRecord(getElderlyPopulation(text), getPopulation(text));
                context.write(new Text(state), record);
            }
        }
    }

    public Long getPopulation(String text) {
        Long population = Long.parseLong(text.substring(300, 309));
        return population;
    }

    public Long getElderlyPopulation(String text) {
        Long elderlyPopulation = Long.parseLong(text.substring(1065, 1074));
        return elderlyPopulation;
    }

}
