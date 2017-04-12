package cs455.RecordUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class RentedOwnedRecord implements Writable {
    private long rented = 0;

    private long owned = 0;

    public RentedOwnedRecord() {

    }

    public long getRented() {
        return rented;
    }

    public void setRented(long rented) {
        this.rented = rented;
    }

    public long getOwned() {
        return owned;
    }

    public void setOwned(long owned) {
        this.owned = owned;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(rented);
        dataOutput.writeLong(owned);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        rented = dataInput.readLong();
        owned = dataInput.readLong();
    }

    @Override
    public String toString() {
        return getTenureString();
    }

    private String getTenureString() {
        double totalHousing = getOwned() + getRented();
        double ownerPercentage = getOwned() / totalHousing * 100;
        double renterPercentage = getRented() / totalHousing * 100;
        return String.format("Total: %s\nOwned: %s %s\tRented: %s %s",
               getOwned() + getRented(),
                getOwned(),
                ownerPercentage,
                getRented(),
                renterPercentage);
    }

}
