package cs455.RecordUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class NonMarriedRecord extends Record {

	private long male = 0;
	private long female = 0;
	private long population = 0;

	public NonMarriedRecord() { }

	public long getNonMarriedMale() {
		return male;
	}

	public void setNonMarriedMale(long male) {
		this.male = male;
	}

	public long getNonMarriedFemale() {
		return female;
	}

	public void setNonMarriedFemale(long female) {
		this.female = female;
	}

	public long getPopulation() {
		return population;
	}

	public void setPopulation(long population) {
		this.population = population;
	}

	@Override
	public String toString() {
		
		return String.format("Non-Married Male: %s %s\tNon-Married Female: %s %s", 
							male, 
							formatDouble(male/(population*1.0)), 
							female, 
							formatDouble(female/(population*1.0)));
	}

	@Override
	public void write(DataOutput output) throws IOException {
		output.writeLong(male);
		output.writeLong(female);
		output.writeLong(population);
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		male = input.readLong();
		female = input.readLong();
		population = input.readLong();
	}
}
