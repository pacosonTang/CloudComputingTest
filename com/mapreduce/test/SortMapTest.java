package com.mapreduce.test;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import pack.WordCount;
import pack.WordCount.Map;

@SuppressWarnings("all")
public class SortMapTest {
	
	private Mapper mapper;
	private MapDriver driver;
	
	@Before
	public void init(){
		mapper = new WordCount.Map();
		driver = new MapDriver(mapper);
	}
	
	@Test
	public void test() throws IOException{
		String line = "this is a test case for map";
		driver.withInput(new LongWritable(1),new Text(line))
				.withOutput(new Text("this"), new IntWritable(1))
				.withOutput(new Text("is"), new IntWritable(1))
				.withOutput(new Text("a"), new IntWritable(1))
				.withOutput(new Text("test"), new IntWritable(1))
				.withOutput(new Text("case"), new IntWritable(1))
				.withOutput(new Text("for"), new IntWritable(1))
				.withOutput(new Text("map"), new IntWritable(1))
				.runTest();
	}
}
