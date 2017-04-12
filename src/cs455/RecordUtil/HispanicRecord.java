package cs455.RecordUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class HispanicRecord extends Record {
    private long male0to18 = 0;
    private long male19to29 = 0;
    private long male30to39 = 0;

    private long female0to18 = 0;
    private long female19to29 = 0;
    private long female30to39 = 0;

    private long totalFemalePopulation = 0;
    private long totalMalePopulation = 0;

    public HispanicRecord() {
    }

    public long getTotalFemalePopulation() {
        return totalFemalePopulation;
    }

    public long getTotalMalePopulation() {
        return totalMalePopulation;
    }

    public void setTotalFemalePopulation(long totalFemalePopulation) {
        this.totalFemalePopulation = totalFemalePopulation;
    }

    public void setTotalMalePopulation(long totalMalePopulation) {
        this.totalMalePopulation = totalMalePopulation;
    }

    public long getMale0to18() {
        return male0to18;
    }

    public void setMale0to18(long male0to18) {
        this.male0to18 = male0to18;
    }

    public long getMale19to29() {
        return male19to29;
    }

    public void setMale19to29(long male19to29) {
        this.male19to29 = male19to29;
    }

    public long getMale30to39() {
        return male30to39;
    }

    public void setFemale30to39(long female30to39) {
        this.female30to39 = female30to39;
    }

    public long getFemale0to18() {
        return female0to18;
    }

    public void setFemale0to18(long female0to18) {
        this.female0to18 = female0to18;
    }

    public long getFemale19to29() {
        return female19to29;
    }

    public void setFemale19to29(long female19to29) {
        this.female19to29 = female19to29;
    }

    public long getFemale30to39() {
        return female30to39;
    }

    public void setMale30to39(long male30to39) {
        this.male30to39 = male30to39;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(male0to18);
        dataOutput.writeLong(male19to29);
        dataOutput.writeLong(male30to39);
        dataOutput.writeLong(female0to18);
        dataOutput.writeLong(female19to29);
        dataOutput.writeLong(female30to39);
        dataOutput.writeLong(totalFemalePopulation);
        dataOutput.writeLong(totalMalePopulation);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        male0to18 = dataInput.readLong();
        male19to29 = dataInput.readLong();
        male30to39 = dataInput.readLong();
        female0to18 = dataInput.readLong();
        female19to29 = dataInput.readLong();
        female30to39 = dataInput.readLong();
        totalFemalePopulation = dataInput.readLong();
        totalMalePopulation = dataInput.readLong();
    }

    @Override
    public String toString() {
        return getString();
    }

    private String getString() {
        
        double maleBelow18Percentage = getMale0to18() / (double) totalMalePopulation * 100;
        double male19to29Percentage = getMale19to29() / (double)totalMalePopulation * 100;
        double male30to39Percentage = getMale30to39() / (double)totalMalePopulation * 100;

        double femaleBelow18Percentage = getFemale0to18() / (double)totalFemalePopulation * 100;
        double female19to29Percentage = getFemale19to29() /(double) totalFemalePopulation * 100;
        double female30to39Percentage = getFemale30to39() /(double) totalFemalePopulation * 100;

        return String.format("Total Male: %s %s %s\tTotal Female: %s %s %s",
							formatDouble(maleBelow18Percentage),
							formatDouble(male19to29Percentage),
							formatDouble(male30to39Percentage),
							formatDouble(maleBelow18Percentage),
							formatDouble(femaleBelow18Percentage),
							formatDouble(female19to29Percentage),
							formatDouble(female30to39Percentage));
    }
}
