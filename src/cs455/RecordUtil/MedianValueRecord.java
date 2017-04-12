package cs455.RecordUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Writable;

public class MedianValueRecord implements Writable {

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

    public MedianValueRecord() {
        houseMap = new LinkedHashMap<>();
        for (String string : VALUE_LIST) {
            houseMap.put(string, 0L);
        }
    }
	public List<String> getValueList() {
		return VALUE_LIST;
	}


    public Map<String, Long> getMap() {
        return houseMap;
    }

    public void setMap(Map<String, Long> houseMap) {
        this.houseMap = houseMap;
    }

    @Override
    public String toString() {
        long total = 0;
        for (Long value : houseMap.values()) {
            total += value;
        }

        long median = total / 2;
        long sum = 0;

        for (Map.Entry<String, Long> entry : houseMap.entrySet()) {
            sum += entry.getValue();
            if (sum >= median) {
                return entry.getKey();
            }
        }

        return VALUE_LIST.get(VALUE_LIST.size() - 1);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        for (Long value : houseMap.values()) {
            dataOutput.writeLong(value);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        for (String string : houseMap.keySet()) {
            houseMap.put(string, dataInput.readLong());
        }
    }
}
