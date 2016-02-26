
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

//Groups values based on LastName (the natural key) of composite key.

public class NaturalKeyGroupingComparator extends WritableComparator {

	/**
	 * Constructor.
	 */
	protected NaturalKeyGroupingComparator() {
		super(StockKey.class, true);
	}
	
	//comparison function for the grouping (notice --- ignore everything other than LastName
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		StockKey k1 = (StockKey)w1;
		StockKey k2 = (StockKey)w2;
		
		return k1.getLastName().compareTo(k2.getLastName());
	}
}
