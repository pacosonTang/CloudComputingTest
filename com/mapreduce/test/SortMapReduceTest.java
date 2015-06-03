package com.mapreduce.test;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.testTwo.SortMapper;
import com.testTwo.SortReduce;

import pack.WordCount;

public class SortMapReduceTest {
	
	private SortReduce reducer;
	private SortMapper mapper;	
	private MapReduceDriver driver;
	
	@Before
	public void init(){
		reducer = new SortReduce();
		mapper = new SortMapper();
		driver = new MapReduceDriver(mapper,reducer);
	}
	@Test
	public void test() throws IOException{
		String line = "chinacache is a great CDN is it not";
		driver.withInput(new LongWritable(1),new Text(line))
				.withOutput(new Text("CDN"), new IntWritable(1))
				.withOutput(new Text("a"), new IntWritable(1))
				.withOutput(new Text("chinacache"), new IntWritable(1))
				.withOutput(new Text("great"), new IntWritable(1))
				.withOutput(new Text("is"), new IntWritable(2))
				.withOutput(new Text("it"), new IntWritable(1))
				.withOutput(new Text("not"), new IntWritable(1))
				.runTest();
	}
	
}
