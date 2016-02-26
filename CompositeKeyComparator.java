
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompositeKeyComparator extends WritableComparator {

	
	// Constructor.

	protected CompositeKeyComparator() {
		super(StockKey.class, true);
	}
	
	//Compare method which handles the comparisons inside a group
	//Current:
	//Natural key: Last Name (Asc)
	//Secondary key: Roll (Desc)
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		StockKey k1 = (StockKey)w1;
		StockKey k2 = (StockKey)w2;
		
		int result = k1.getLastName().compareTo(k2.getLastName());
		if(0 == result) {
			result = -1* k1.getRoll().compareTo(k2.getRoll());
		}
		return result;
	}
}
