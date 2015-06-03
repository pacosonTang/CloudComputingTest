package com.join;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.inject.Key;

public class JoinReduce extends Reducer<TextPaire, Text, Text, Text>{

	@Override
	protected void reduce(TextPaire key, Iterable<Text> values,
			Context context)
			throws IOException, InterruptedException {
		//得到的 values 是无序的，再次排序。
		
		Iterator<Text> it = values.iterator(); 
		String tradeId = it.next().toString();//first value is tradeID
		System.out.println("reduce: key = " + key.toString());
		while(it.hasNext()) {
			String payId = it.next().toString();//first value is tradeID
			context.write(new Text(tradeId), new Text(payId));
		}
		/*System.out.println("reduce : TextPaire.key " + key.toString() + ", tradeId = " + tradeId);*/
	}
}
