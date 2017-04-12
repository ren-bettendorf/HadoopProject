package cs455.RecordUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ComparisonRecord extends Record {
	
	public final List<String> VALUE_LIST = Arrays.asList("< 15k",
														"15k - 20k",
														"20k - 25k",
														"25k - 30k",
														"30k - 35k",
														"35k - 40k",
														"40k - 45k",
														"45k - 50k",
														"50k - 60k",
														"60k - 75k",
														"75k - 100k",
														"100k - 125k",
														"125k - 150k",
														"150k - 175k",
														"175k - 200k",
														"200k - 250k",
														"250k - 300k",
														"300k - 400k",
														"400k - 500k",
														"500k >");

    private Map<String, Long> houseMap;
	
	public final int ROOM_SIZE = 9;

    private long[] roomCounts;

    public MedianValueRecord() {
        houseMap = new LinkedHashMap<>();
        for (String string : VALUE_LIST) {
            houseMap.put(string, 0L);
        }
		roomCounts = new long[ROOM_SIZE];
    }
	public List<String> getValueList() {
		return VALUE_LIST;
	}


    public Map<String, Long> getMap() {
        return houseMap;
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
	
    public void setMap(Map<String, Long> houseMap) {
        this.houseMap = houseMap;
    }
	
	public void setAverageRoom(String averageRoom) {
		this.averageRoom = averageRoom;
	}

    @Override
    public String toString() {
        long total = 0;
        for (Long value : houseMap.values()) {
            total += value;
        }

        long median = total / 2;
        long sum = 0;
		String medianValue = "";
        for (Map.Entry<String, Long> entry : houseMap.entrySet()) {
            sum += entry.getValue();
            if (sum >= median) {
                medianValue = entry.getKey();
				break;
            }else if(entry.getKey().equals(VALUE_LIST.get(VALUE_LIST.size()-1))) {
				medianValue = entry.getKey();
			}
        }
		
        return String.format("%s, %s",
							medianValue, 
							);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        for (Long value : houseMap.values()) {
            dataOutput.writeLong(value);
        }
		for (int i = 0; i < ROOM_SIZE; i++) {
            dataOutput.writeLong(roomCounts[i]);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        for (String string : houseMap.keySet()) {
            houseMap.put(string, dataInput.readLong());
        }
		for (int i = 0; i < ROOM_SIZE; i++) {
            roomCounts[i] = dataInput.readLong();
        }
    }
}
