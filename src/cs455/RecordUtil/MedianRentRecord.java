package cs455.RecordUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;

import org.apache.hadoop.io.Writable;

public class MedianRentRecord implements Writable {

    public final List<String> RENT_LIST = new ArrayList<String>(Arrays.asList("< 100",
            "100 - 150",
            "150 - 199",
            "200 - 249",
            "250 - 299",
            "300 - 349",
            "350 - 399",
            "400 - 449",
            "450 - 499",
            "500 - 549",
            "550 - 599",
            "600 - 649",
            "650 - 699",
            "700 - 749",
            "750 - 999",
            "1000 >"));

    private Map<String, Long> map;

    public MedianRentRecord() {
        this.map = new LinkedHashMap<String, Long>();
	for (String priceRange: RENT_LIST) {
		map.put(priceRange, 0L);
	}
    }
	public List<String> getRentList() {
		return RENT_LIST;
	}

    public Map<String, Long> getMap() {
        return map;
    }

    public void setMap(Map<String, Long> map) {
        this.map = map;
    }

    @Override
    public String toString() {
        long total = 0;
        for (Long value : map.values()) {
            total += value;
        }

        long median = total / 2;
        long result = 0;

        for (Map.Entry<String, Long> entry : map.entrySet()) {
            if (result + entry.getValue() >=  median) {
                return entry.getKey();
            }
            result += entry.getValue();
        }

        return RENT_LIST.get(RENT_LIST.size() - 1);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        for (Long value : map.values()) {
            dataOutput.writeLong(value);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        for (String priceRange : map.keySet()) {
            map.put(priceRange, dataInput.readLong());
        }
    }
}
