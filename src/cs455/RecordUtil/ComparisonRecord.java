package cs455.RecordUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class ComparisonRecord extends Record {

	private long urban = 0L;
	private long total = 0L;
    private final int ROOM_SIZE = 9;
	private long averageRoom = 0L;
    private long[] roomCounts;

    public ComparisonRecord() {
        roomCounts = new long[ROOM_SIZE];
    }
    public int getNumberRooms() {
		return ROOM_SIZE;
	}
    public long[] getRoomCounts() {
        return roomCounts;
    }

    public void setRoomCounts(long[] roomCounts) {
        this.roomCounts = roomCounts;
    }
	
	public long getUrbanPopulation() {
		return urban;
	}
	
	public long getTotalPopulation() {
		return total;
	}
	
	public void setAverageRoom(long averageRoom) {
		this.averageRoom = averageRoom;
	}
	
	public void setUrbanPopulation(long urban) {
		this.urban = urban;
	}
	
	public void setTotalPopulation(long total) {
		this.total = total;
	}
	
	@Override
	public String toString() {
		double urbanPercentage = urban / (double)(total) * 100;
        return String.format("%s %s",
							formatDouble(urbanPercentage),
							averageRoom);
	}

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        for (int i = 0; i < ROOM_SIZE; i++) {
            dataOutput.writeLong(roomCounts[i]);
        }
		dataOutput.writeLong(urban);
		dataOutput.writeLong(total);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        for (int i = 0; i < ROOM_SIZE; i++) {
            roomCounts[i] = dataInput.readLong();
        }
		this.urban = dataInput.readLong();
		this.total = dataInput.readLong();
    }
}
