package cs455.RecordUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class ElderlyRecord extends Record {

    private long totalPopulation = 0;
    private long elderlyPopulation = 0;

    public ElderlyRecord() {
    }

    public void setTotalPopulation(long totalPopulation) {
		this.totalPopulation = totalPopulation;
    }
	
	public void setElderlyPopulation(long elderlyPopulation) {
		this.elderlyPopulation = elderlyPopulation;
	}

    public long getElderlyPopulation() {
        return elderlyPopulation;
    }

    public long getTotalPopulation() {
        return totalPopulation;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(totalPopulation);
        dataOutput.writeLong(elderlyPopulation);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.totalPopulation = dataInput.readLong();
        this.elderlyPopulation = dataInput.readLong();
    }

}
