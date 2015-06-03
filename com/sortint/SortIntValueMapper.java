package com.sortint;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.inject.Key;

/**
 * 把上一个mapreduce的结果的key和value颠倒，调到后就可以按照key排序了。
 * @author zhangdonghao
 */
public class SortIntValueMapper extends
		Mapper<LongWritable, Text, IntWritable, Text> {
	private final static IntWritable wordCount = new IntWritable(1);

	private Text word = new Text();

	public SortIntValueMapper() {
		super();
		System.out.println("SortIntValueMapper 构造方法");
	}

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		System.out.println("SortIntValueMapper.map() 方法");
		StringTokenizer tokenizer = new StringTokenizer(value.toString());
		System.out.println("key = " + key + "  value = " + value);
		while (tokenizer.hasMoreTokens()) {
			word.set(tokenizer.nextToken().trim());
			wordCount.set(Integer.valueOf(tokenizer.nextToken().trim()));
			context.write(wordCount, word);
		}
	}
}