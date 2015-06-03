package com.testTwo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class PartitionByText  extends Partitioner<IntPaire, IntWritable>{

	//numPartitions == reduce nums
	@Override
	public int getPartition(IntPaire key, IntWritable value, int numPartitions) {
		
		return (key.getFirstKey().hashCode() & Integer.MAX_VALUE) % numPartitions;
		
	}
	
}
