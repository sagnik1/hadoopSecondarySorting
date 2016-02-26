
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// Secondary sort mapper.
 
public class SsMapper extends Mapper<LongWritable, Text, StockKey, Text> {

	//emits the composite key (StockKey) as key and Text as value
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] tokens = value.toString().split(",");
		if(tokens.length==3){
			String FN = tokens[0].trim();
			String LN = tokens[1].trim();
			Integer R = Integer.parseInt(tokens[2].trim());
		
			StockKey stockKey = new StockKey(FN,LN,R);
			Text stockValue = new Text(value);
		
			context.write(stockKey, stockValue);
		}
	
	}
}
