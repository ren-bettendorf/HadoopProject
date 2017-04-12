package cs455.RecordUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Record implements Writable {

	private long logicalRecordPart = 0;
	private long totalParts = 0;

	public Record() { }

	public void setLogicalRecordPart(long logicalRecordPart) {
		this.logicalRecordPart = logicalRecordPart;
	}

	public void setTotalParts(long totalParts) {
		this.totalParts = totalParts;
	}

	public long getLogicalRecordPart() {
		return logicalRecordPart;
	}

	public long getTotalParts() {
		return totalParts;
	}

	@Override
	public void write(DataOutput output) throws IOException { }

	@Override
	public void readFields(DataInput input) throws IOException { }
}
