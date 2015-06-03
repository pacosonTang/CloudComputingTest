package com.join;

import java.io.IOException;

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
	protected void map(Object key, Text value, //str1[key]  5[value]
			Context context) 
			throws IOException, InterruptedException {
		
		// path => /usr/hadoop/1503061934/action.txt
		FileSplit fs = (FileSplit)context.getInputSplit();
		String path = fs.getPath().toString();
		
		String[] line = value.toString().split("\"");//action.txt alipay.txt
		if(line.length < 2)
			return ;
		TextPaire tp = new TextPaire();
		String pid = line[0];
		tp.setText(pid);
		
		//set product id
		Text kv = new Text();
		if(path.contains("action")) {//商品表
			// trade table
			tp.setId(0);
			// 用于TextPaire 的排序
			String tradeID = line[1];
			kv.set(tradeID);
			// value is tradeID
		}
		else if(path.contains("alipay")) {//支付表
			//pay table
			tp.setId(1);
			//用于TextPaire 的排序
			String payId = line[1];
			kv.set(payId);
			//value is payID
		}
		context.write(tp, kv);//key(str1 5) value(5)
	}
}
