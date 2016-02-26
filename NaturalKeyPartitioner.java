

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

//Partitions key based on natural key of Composite Key (which is the LastName).
 
public class NaturalKeyPartitioner extends Partitioner<StockKey, IntWritable> {

	@Override
	public int getPartition(StockKey key, IntWritable val, int numPartitions) {
		int hash = key.getLastName().hashCode();
		int partition = hash % numPartitions;
		return partition;
	}

}
