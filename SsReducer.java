
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// Secondary sort reducer.

public class SsReducer extends Reducer<StockKey, Text, Text, Text> {

	@Override
	public void reduce(StockKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Text k = new Text(" ");
		int count = 0;
		
		Iterator<Text> it = values.iterator();
		while(it.hasNext()) {
			Text v = new Text(it.next().toString());
			context.write(k, v);
		
			count++;
		}
		
	}
}
