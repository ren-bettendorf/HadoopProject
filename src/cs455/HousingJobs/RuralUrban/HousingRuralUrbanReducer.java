package cs455.HousingJobs.RuralUrban;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import cs455.RecordUtil.HousingRecord;

public class HousingRuralUrbanReducer extends Reducer<Text, HousingRecord, Text, HousingRecord> {
	@Override
	protected void reduce(Text key, Iterable<HousingRecord> values, Context context) throws IOException, InterruptedException {
		long rural = 0;
		long urban = 0;

		for(HousingRecord val : values){
			rural += val.getRural();
			urban += val.getUrban();
		}
		HousingRecord record = new HousingRecord();
		record.setRural(rural);
		record.setUrban(urban);

		context.write(key, record);
	}
}
