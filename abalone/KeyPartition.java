package com.variance.abalone;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class KeyPartition  extends Partitioner<TextPaire, Text>{

	@Override
	public int getPartition(TextPaire key, Text value, int numPartitions) {
		
		return (key.getFirstKey().hashCode() & Integer.MAX_VALUE) % numPartitions;
		
	} 
}
