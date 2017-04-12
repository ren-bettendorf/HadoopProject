package cs455.HousingJobs.RuralUrban;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import cs455.RecordUtil.HousingRecord;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives word, list<count> pairs.
 * Sums up individual counts per given word. Emits <word, total count> pairs.
 */
public class HousingRuralUrbanReducer extends Reducer<Text, HousingRecord, Text, HousingRecord> {
    	@Override
    	protected void reduce(Text key, Iterable<HousingRecord> values, Context context) throws IOException, InterruptedException {
        	long rural = 0;
        	long urban = 0;
		long totalParts = 0;
		long logicalRecordPart = 0;

        	for(HousingRecord val : values){
            		rural += val.getRural();
			urban += val.getUrban();
			totalParts = val.getTotalParts();
			logicalRecordPart = val.getLogicalRecordPart();
        	}
		HousingRecord housingRecord = new HousingRecord();
		housingRecord.setRural(rural);
		housingRecord.setUrban(urban);
		housingRecord.setLogicalRecordPart(logicalRecordPart);
		housingRecord.setTotalParts(totalParts);

        	context.write(key, housingRecord);
	}
}
