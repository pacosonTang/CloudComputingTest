package com.probability.abalone;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("all")
public class MapTest {
	
	private ProbabilityMapper mapper;
	private MapDriver driver;
	
	@Before
	public void init(){
		mapper = new ProbabilityMapper();
		driver = new MapDriver(mapper);
	}
	 
	@Test
	public void test() throws IOException, URISyntaxException{
		
		Configuration conf = new Configuration();
		String line = "M,0.455,0.365,0.095,0.514,0.2245,0.101,0.15,15";
		String[] arr = line.split(",");
		/*
		DistributedCache.addCacheFile(new URI("hdfs://192.168.119.105:9000/test/meanVarianceAbalone/part-r-00000#meanVarianceAbalone"), conf);
		
		//添加hdfs的文件meanVarianceAbalone到分布式缓存
		DistributedCache.addCacheFile(new URI("hdfs://192.168.119.105:9000/test/meanAbalone/part-r-00000#meanAbalone"), conf);
		*/
		driver.withInput(new Text(),new Text(line))
				.withOutput(new Text("15@length2"), new Text(arr[1]))
				.runTest();
	}
}
