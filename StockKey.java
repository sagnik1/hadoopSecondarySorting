
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Stock key. This key is a composite key. The "natural"
 * key is the FirstName. The secondary sort will be performed
 * against the Roll.
 *
 */
public class StockKey implements WritableComparable<StockKey> {
	
	//Fields are used together as composite key... at the moment only LastName and Roll are relevant... 
	//but firstname also available
	//To change grouping modify the comparator and partitioner classes
	private String FirstName;
	private String LastName;
	private Integer Roll;
	
	public StockKey() { }
	
	
	public StockKey(String FN, String LN, Integer R) {
		this.FirstName = FN ;
		this.LastName = LN;
		this.Roll = R;
	}
	
	@Override
	public String toString() {
		return (new StringBuilder())
				.append(' ')
				.append(FirstName)
				.append(' ')
				.append(LastName)
				.append(' ')
				.toString();
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		FirstName = WritableUtils.readString(in);
		LastName = WritableUtils.readString(in);
		Roll = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, FirstName);
		WritableUtils.writeString(out, LastName);
		out.writeInt(Roll);
	}

	@Override
	public int compareTo(StockKey o) {
		int result = LastName.compareTo(o.getLastName());
		if(0 == result) {
			result = Roll.compareTo(o.getRoll());
		}
		return result;
	}

	/**
	 * Typical Getter Setter for the composite key attributes
	 * 
	 */
	public String getFirstName() {
		return FirstName;
	}

	public void setFirstName(String FN) {
		this.FirstName = FN;
	}
	
	public String getLastName() {
		return LastName;
	}

	public void setLastName(String FN) {
		this.LastName = FN;
	}

	public Integer getRoll() {
		return Roll;
	}

	public void setRoll(Integer R) {
		this.Roll = R;
	}



}
