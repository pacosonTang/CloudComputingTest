package com.variance.abalone;

import java.io.IOException;
import java.nio.file.Path;

import javax.sound.sampled.Line;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.I;
import org.mockito.asm.tree.analysis.Value;

/**
 * 排序mapper
 * 
 * @author Tang Rong (Pacoson Tang)
 * 
 */
public class VarianceMapper extends Mapper<Object, Text, TextPaire, Text>{

	@Override
	protected void map(Object key, Text value,
			Context context) 
			throws IOException, InterruptedException {
		
		// path => /usr/hadoop/1503061934/action.txt
		FileSplit fs = (FileSplit)context.getInputSplit();
		String path = fs.getPath().toString();
		System.out.println("path = " + path);
		
		//我们不会对sex（male, female, infant）和rings 求样本均值；（他们是离散型数据）
		if(value.toString().contains("male") || value.toString().contains("infant") || value.toString().contains("rings"))
			return;
		
		String[] line = value.toString().split(",");
		TextPaire tp = null;
		String keystr = line[0].trim(); 
		if(keystr.contains("@")) {  //说明是均值数据行, 1就表示 它是某属性的均值
			tp = new TextPaire(line[0].trim(), "1");
			context.write(tp, new Text(line[1]));
		}
		else {//说明是样本数据行
//sample structure:	Sex(M,F,I), Length,Diameter,Height,Whole weight,Shucked weight,Viscera weight,Shell weight,Rings
		/*	Text length = new Text(line[8]+"@length"); //length
			Text diameter = new Text(line[8]+"@diameter"); //diameter 
			Text height = new Text(line[8]+"@height"); //height
			Text whole_weight = new Text(line[8]+"@whole_weight"); //whole_weight 
			Text shucked_weight = new Text(line[8]+"@shucked_weight"); //shucked_weight 
			Text viscera_weight = new Text(line[8]+"@viscera_weight"); //viscera_weight 
			Text shell_weight = new Text(line[8]+"@shell_weight"); //shell_weight
			Text rings = new Text(line[8]+"@rings"); //rings*/
			
			String[] properties = {"@length","@diameter","@height","@whole_weight","@shucked_weight",
									"@viscera_weight","@shell_weight"};
			for(int i = 1; i < line.length - 1; i++) {
				tp = new TextPaire(line[8]+properties[i-1], "2");//2就表示它是某属性的样本值 
				context.write(tp, new Text(line[i]));
			}
			/**
			 * 对于means.txt 每行数据：  <'key 1',value> <10@diameter	1, 0.062296707>
			   对于sample.txt 每行数据： <'key 2',value> <10@diameter	2, 0.455>
			 */
		}
	}
}
