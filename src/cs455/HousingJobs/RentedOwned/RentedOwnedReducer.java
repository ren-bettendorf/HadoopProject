package cs455.HousingJobs.RentedOwned;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import cs455.RecordUtil.RentedOwnedRecord;

public class RentedOwnedReducer extends Reducer<Text, RentedOwnedRecord, Text, RentedOwnedRecord> {
    @Override
    protected void reduce(Text key, Iterable<RentedOwnedRecord> values, Context context) throws IOException, InterruptedException {
        long rented = 0;
        long owned = 0;

        for (RentedOwnedRecord val : values) {
            rented += val.getRented();
            owned += val.getOwned();
        }

		RentedOwnedRecord record = new RentedOwnedRecord();
        record.setOwned(owned);
        record.setRented(rented);
        context.write(key, record);
    }

}
