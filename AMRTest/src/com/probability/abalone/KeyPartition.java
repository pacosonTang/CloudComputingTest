package com.probability.abalone;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class KeyPartition  extends Partitioner<Text, IntWritable>{

	@Override
	public int getPartition(Text key, IntWritable value, int numPartitions) {
		
		return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
		
	} 
}
