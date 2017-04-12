package cs455.SocialJobs.Hispanic;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import cs455.RecordUtil.HispanicRecord;

public class HispanicReducer extends Reducer<Text, HispanicRecord, Text, HispanicRecord> {
    @Override
    protected void reduce(Text key, Iterable<HispanicRecord> values, Context context) throws IOException, InterruptedException {
        long male0to18 = 0;
        long male19to29 = 0;
        long male30to39 = 0;

        long female0to18 = 0;
        long female19to29 = 0;
        long female30to39 = 0;

        long totalFemalePopulation = 0;
        long totalMalePopulation = 0;

        for (HispanicRecord val : values) {
			// Compute male values
            male0to18 += val.getMale0to18();
            male19to29 += val.getMale19to29();
            male30to39 += val.getMale30to39();

			// Compute female values
            female0to18 += val.getFemale0to18();
            female19to29 += val.getFemale19to29();
            female30to39 += val.getFemale30to39();

			// Compute total values
            totalFemalePopulation += val.getTotalFemalePopulation();
            totalMalePopulation += val.getTotalMalePopulation();
        }

        HispanicRecord record = new HispanicRecord();
		
		// Set all male values
        record.setMale0to18(male0to18);
        record.setMale19to29(male19to29);
        record.setMale30to39(male30to39);
		
		// Set all female values
        record.setFemale0to18(female0to18);
        record.setFemale19to29(female19to29);
        record.setFemale30to39(female30to39);
		
		// Set all total values
        record.setTotalMalePopulation(totalMalePopulation);
        record.setTotalFemalePopulation(totalFemalePopulation);
		
        context.write(key, record);
    }
}
