package cs455.RecordUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class AverageRoomRecord implements Writable {

    public final int ROOM_SIZE = 9;

    private long[] roomCounts;

    public AverageRoomRecord() {
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

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        for (int i = 0; i < ROOM_SIZE; i++) {
            dataOutput.writeLong(roomCounts[i]);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        for (int i = 0; i < ROOM_SIZE; i++) {
            roomCounts[i] = dataInput.readLong();
        }
    }
}
