package com.join;

import java.io.IOException;
import java.nio.file.Path;

import javax.sound.sampled.Line;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.mockito.asm.tree.analysis.Value;

/**
 * 排序mapper
 * 
 * @author Tang Rong (Pacoson Tang)
 * 
 */
public class JoinMapper extends Mapper<Object, Text, TextPaire, Text>{

	@Override
	protected void map(Object key, Text value,
			Context context) 
			throws IOException, InterruptedException {
		
		// path => /usr/hadoop/1503061934/action.txt
		//FileSplit fs = (FileSplit)context.getInputSplit();
		//String path = fs.getPath().toString();
		 
		String[] line = key.toString().split("\"");// 因为它采用KeyValueTextInputFormat 所以key就是行数据
		if(line.length < 2)
			return ;
		TextPaire tp = new TextPaire(line[0], line[1]);
		System.out.println("map : " + "key = " + tp.toString() + ", value = " + line[1]);
		context.write(tp, new Text(line[1]));//<"product1,trade1","trade1"> or <"product1,pay1","pay1"> 
	}
}
