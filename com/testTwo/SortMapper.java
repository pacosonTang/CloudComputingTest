package com.testTwo;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.mockito.asm.tree.analysis.Value;

import com.google.inject.Key;

/**
 * 排序mapper
 * 
 * @author Tang Rong (Pacoson Tang)
 *
 */
public class SortMapper extends Mapper<Text, Text, IntPaire, IntWritable>{

	public IntPaire ip = new IntPaire();
	public IntWritable iw = new IntWritable(0);
	
	@Override
	protected void map(Text key, Text value, //str1[key]  5[value]
			Context context) 
			throws IOException, InterruptedException {
		System.out.println("key = " + key + ", value = " + value);
		int intValue = Integer.parseInt(value.toString());
		
		ip.setFirstKey(key.toString());//str1 [firstKey]
		ip.setSecondKey(intValue);//5 [secondKey]
		iw.set(intValue);//5 [value]
		context.write(ip, iw);//key(str1 5) value(5)
	}
}
