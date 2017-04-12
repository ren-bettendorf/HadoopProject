package cs455.RecordUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class HousingRecord extends Record {

	private long rural = 0;
	private long urban = 0;

	public HousingRecord() { }

	public long getRural() {
		return rural;
	}

	public void setRural(long rural) {
		this.rural = rural;
	}

	public long getUrban() {
		return urban;
	}

	public void setUrban(long urban) {
		this.urban = urban;
	}

	@Override
	public String toString() {
		
		return String.format("Rural: %s\tUrban: %s",
							rural,
							urban);
	}

	@Override
	public void write(DataOutput output) throws IOException {
		output.writeLong(rural);
		output.writeLong(urban);
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		rural = input.readLong();
		urban = input.readLong();
	}
}
