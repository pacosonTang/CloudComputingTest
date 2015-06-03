package com.sortint;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 把key和value颠倒过来输出
 * @author zhangdonghao
 */
public class SortIntValueReduce extends
		Reducer<IntWritable, Text, Text, IntWritable> {

	private Text result = new Text();

	public SortIntValueReduce() {
		super();
		System.out.println("SortIntValueReduce 的构造方法");
	}

	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		System.out.println("SortIntValueReduce.reduce 方法" + ", Reduce.class = " + this.toString());
		for (Text val : values) {
			System.out.println("key = " + key + "  value = " + val.toString());
			result.set(val.toString());
			context.write(result, key);
		}
	}
}