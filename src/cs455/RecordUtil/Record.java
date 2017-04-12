package cs455.RecordUtil;

import java.text.DecimalFormat;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Record implements Writable {

	public Record() { }

	@Override
	public void write(DataOutput output) throws IOException { }

	@Override
	public void readFields(DataInput input) throws IOException { }
	
	private double formatDouble(double input) {
		DecimalFormat decimalFormatter = new DecimalFormat("#.00");
		return decimalFormatter.format(input);
	}
}
