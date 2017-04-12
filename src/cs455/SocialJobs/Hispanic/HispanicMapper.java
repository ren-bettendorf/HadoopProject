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
            Long logicalRecordPart = Long.parseLong(text.substring(24,28));
            Long totalParts = Long.parseLong(text.substring(28,32));

            HispanicRecord record = new HispanicRecord();

            if (!logicalRecordPart.equals(totalParts)) {
                record.setMaleHispanic0to18(getMaleHispanicBelow18(text));
                record.setFemaleHispanic0to18(getFemaleHispanicBelow18(text));

                record.setMaleHispanic19to29(getMaleHispanic19to29(text));
                record.setFemaleHispanic19to29(getFemaleHispanic19to29(text));

                record.setMaleHispanic30to39(getMaleHispanic30to39(text));
                record.setFemaleHispanic30to39(getFemaleHispanic30to39(text));

                record.setTotalMaleHispanicPopulation(getTotalMalePopulation(text));
                record.setTotalFemaleHispanicPopulation(getTotalFemalePopulation(text));
                context.write(new Text(state), record);
            }
        }
    }

    public Long getMaleHispanicBelow18(String unparsedText) {
        return parseText(3864, 3973, unparsedText);
    }

    public Long getFemaleHispanicBelow18(String unparsedText) {
        return parseText(4143, 4252, unparsedText);
    }

    public Long getMaleHispanic19to29(String unparsedText) {
        return parseText(3981, 4018, unparsedText);
    }

    public Long getFemaleHispanic19to29(String unparsedText) {
        return parseText(4260, 4297, unparsedText);
    }

    public Long getMaleHispanic30to39(String unparsedText) {
        return parseText(4026, 4036, unparsedText);
    }

    public Long getFemaleHispanic30to39(String unparsedText) {
        return parseText(4305, 4315, unparsedText);
    }

    public long getTotalMalePopulation(String unparsedText) {
        return parseText(3864, 4135, unparsedText);
    }

    public long getTotalFemalePopulation(String unparsedText) {
        return parseText(4143, 4414, unparsedText);
    }

    private long parseText(int start, int end, String unparsedText) {
        long result = 0L;
        for (int i = start; i < end; i += 9) {
            Long reading = Long.parseLong(unparsedText.substring(i, i + 9));
            result += reading;
        }

        return result;
    }
}
