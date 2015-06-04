package com.variance.abalone;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("all")
public class MapTest {
	
	private VarianceMapper mapper;
	private MapDriver driver;
	
	@Before
	public void init(){
		mapper = new VarianceMapper();
		driver = new MapDriver(mapper);
	}
	String[] properties = {"@length","@diameter","@height","@whole_weight","@shucked_weight",
			"@viscera_weight","@shell_weight", "@rings"};
	
	@Test
	public void test() throws IOException{
		String line = "M,0.455,0.365,0.095,0.514,0.2245,0.101,0.15,15";
		String[] arr = line.split(",");
		driver.withInput(new Text(),new Text(line))
				.withOutput(new TextPaire("15@length","2"), new Text(arr[1]))
				.withOutput(new TextPaire("15@length","2"), new Text(arr[2]))
				.withOutput(new TextPaire("15@length","2"), new Text(arr[3]))
				.withOutput(new TextPaire("15@length","2"), new Text(arr[4]))
				.withOutput(new TextPaire("15@length","2"), new Text(arr[5]))
				.withOutput(new TextPaire("15@length","2"), new Text(arr[6]))
				.withOutput(new TextPaire("15@length","2"), new Text(arr[7]))
				.runTest();
	}
}
