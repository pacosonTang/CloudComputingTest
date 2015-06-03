package com.mapreduce.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import pack.WordCount;
import pack.WordCount.Reduce;

public class ReduceTest {
	
	private Reducer reducer;
	private ReduceDriver driver;
	
	@Before
	public void init(){
		reducer = new WordCount.Reduce();
		driver = new ReduceDriver(reducer);
		
	}
	@Test
	public void test() throws IOException{
		String key = "test";
		List<IntWritable> values = new ArrayList();
		values.add(new IntWritable(2));
		values.add(new IntWritable(3));//5
		
		driver.withInput(new Text(key),values)
				.withOutput(new Text("test"), new IntWritable(5))
				.runTest();
	}
	
}
