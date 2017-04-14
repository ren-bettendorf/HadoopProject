package cs455.SocialJobs.Hispanic;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cs455.RecordUtil.HispanicRecord;

public class HispanicMapper extends Mapper<LongWritable, Text, Text, HispanicRecord> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String text = value.toString();
        String summary = text.substring(10,13);

        if (summary.equals("100")) {

            String state = text.substring(8,10);

            HispanicRecord record = new HispanicRecord();

            Long logicalRecordPart = Long.parseLong(text.substring(24,28));
			Long totalParts = Long.parseLong(text.substring(28,32));
			
			if(!logicalRecordPart.equals(totalParts)) {
				record.setMale0to18(parseMaleBelow18(text));
				record.setFemale0to18(parseFemaleBelow18(text));

				record.setMale19to29(parseMale19to29(text));
				record.setFemale19to29(parseFemale19to29(text));

				record.setMale30to39(parseMale30to39(text));
				record.setFemale30to39(parseFemale30to39(text));

				record.setTotalMalePopulation(parseTotalMalePopulation(text));
				record.setTotalFemalePopulation(parseTotalFemalePopulation(text));
				context.write(new Text(state), record);
			}
        }
    }

    public Long parseMaleBelow18(String text) {
        return parseCompleteAgeGap(text, 3864, 3973);
    }

    public Long parseFemaleBelow18(String text) {
        return parseCompleteAgeGap(text, 4143, 4252);
    }

    public Long parseMale19to29(String text) {
        return parseCompleteAgeGap(text, 3981, 4018);
    }

    public Long parseFemale19to29(String text) {
        return parseCompleteAgeGap(text, 4260, 4297);
    }

    public Long parseMale30to39(String text) {
        return parseCompleteAgeGap(text, 4026, 4036);
    }

    public Long parseFemale30to39(String text) {
        return parseCompleteAgeGap(text, 4305, 4315);
    }

    public long parseTotalMalePopulation(String text) {
        return parseCompleteAgeGap(text, 3864, 4135);
    }

    public long parseTotalFemalePopulation(String text) {
        return parseCompleteAgeGap(text, 4143, 4414);
    }
	
	private Long parseCompleteAgeGap(String text, int start, int end) {
		Long total = 0L;
		for(int index = start; index < end; index += 9) {
			total += Long.parseLong(text.substring(index, index + 9));
		}
		return total;
	}
}
