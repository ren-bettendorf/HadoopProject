package cs455.SocialJobs.ElderlyPopulation;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import cs455.RecordUtil.ElderlyRecord;

public class ElderlyReducer extends Reducer<Text, ElderlyRecord, Text, DoubleWritable> {
	
    private String state = "";
    private double percent = -1.0;

    @Override
    protected void reduce(Text key, Iterable<ElderlyRecord> values, Context context) throws IOException, InterruptedException {

        for (ElderlyRecord val : values) {
            double statePercent = (val.getElderlyPopulation() / (double)val.getTotalPopulation()) * 100;
            if (statePercent > percent) {
                percent = statePercent;
                state = key.toString();
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(new Text(state), new DoubleWritable(percent));
    }
}
