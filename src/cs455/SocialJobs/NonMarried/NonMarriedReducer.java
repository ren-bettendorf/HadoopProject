package cs455.SocialJobs.NonMarried;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import cs455.RecordUtil.NonMarriedRecord;

public class NonMarriedReducer extends Reducer<Text, NonMarriedRecord, Text, NonMarriedRecord> {
	@Override
	protected void reduce(Text key, Iterable<NonMarriedRecord> values, Context context) throws IOException, InterruptedException {
		long male = 0;
		long female = 0;
		long population = 0;

		for(NonMarriedRecord val : values){
			male += val.getNonMarriedMale();
			female += val.getNonMarriedFemale();
			population += val.getPopulation();
		}
		NonMarriedRecord record = new NonMarriedRecord();
		record.setNonMarriedMale(male);
		record.setNonMarriedFemale(female);
		record.setPopulation(population);

		context.write(key, record);
	}	
}
