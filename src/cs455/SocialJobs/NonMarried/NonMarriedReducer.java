package cs455.SocialJobs.NonMarried;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import cs455.RecordUtil.NonMarriedRecord;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives word, list<count> pairs.
 * Sums up individual counts per given word. Emits <word, total count> pairs.
 */
public class NonMarriedReducer extends Reducer<Text, NonMarriedRecord, Text, NonMarriedRecord> {
	@Override
	protected void reduce(Text key, Iterable<NonMarriedRecord> values, Context context) throws IOException, InterruptedException {
		long male = 0;
		long female = 0;
		long population = 0;
		long totalParts = 0;
		long logicalRecordPart = 0;

		for(NonMarriedRecord val : values){
			male += val.getNonMarriedMale();
			female += val.getNonMarriedFemale();
			population += val.getPopulation();
			totalParts = val.getTotalParts();
			logicalRecordPart = val.getLogicalRecordPart();
		}
		NonMarriedRecord record = new NonMarriedRecord();
		record.setNonMarriedMale(male);
		record.setNonMarriedFemale(female);
		record.setPopulation(population);

		context.write(key, record);
	}	
}
