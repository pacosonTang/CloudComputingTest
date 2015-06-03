package com.join;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class KeyPartition  extends Partitioner<TextPaire, Text>{

	@Override
	public int getPartition(TextPaire key, Text value, int numPartitions) {
		
		return (key.getProductId().hashCode() & Integer.MAX_VALUE) % numPartitions;
		
	} 
}
