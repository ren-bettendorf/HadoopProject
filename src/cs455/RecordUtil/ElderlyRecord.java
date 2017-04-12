package cs455.RecordUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class ElderlyRecord implements Writable {

    private double totalPopulation = 0.0;
    private double elderlyPopulation = 0.0;

    public ElderlyRecord() {

    }

    public void setPopulation(double elderlyPopulation, double totalPopulation) {
		this.totalPopulation = totalPopulation;
		this.elderlyPopulation = elderlyPopulation;
    }

    public double getElderlyPopulation() {
        return elderlyPopulation;
    }

    public double getTotalPopulation() {
        return totalPopulation;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(totalPopulation);
        dataOutput.writeDouble(elderlyPopulation);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.totalPopulation = dataInput.readDouble();
        this.elderlyPopulation = dataInput.readDouble();
    }

}
