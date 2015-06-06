package com.probability.abalone;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/**
 * 自定义类型
 * 
 * @author Tang Rong (Pacoson Tang)
 *
 */
public class ProbabilityReduce extends Reducer<Text, IntWritable, Text, FloatWritable>{

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context)
			throws IOException, InterruptedException {
		
		Iterator<IntWritable> it = values.iterator(); 
		System.out.println("reduce method starting"); 
		float sum = 0;
		while(it.hasNext())  // key就只有两个值0-true，1-false
			 sum += Float.parseFloat(it.next().toString());
		
		context.write(new Text(key), new FloatWritable(sum/2089));
	}
}
