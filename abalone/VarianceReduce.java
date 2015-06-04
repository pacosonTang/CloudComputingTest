package com.variance.abalone;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.math3.stat.descriptive.summary.Sum;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.inject.Key;
import com.sun.org.apache.xpath.internal.operations.String;
/**
 * 自定义类型
 * 
 * @author Tang Rong (Pacoson Tang)
 *
 */
public class VarianceReduce extends Reducer<TextPaire, Text, Text, FloatWritable>{

	@Override
	protected void reduce(TextPaire key, Iterable<Text> values,
			Context context)
			throws IOException, InterruptedException {
		//得到的 values 是无序的，再次排序。
		
		Iterator<Text> it = values.iterator(); 
		float singleMean = Float.parseFloat(it.next().toString()); //排在第一的就是均值，后面跟的是sample马仔
		//System.out.println("reduce: key = " + key.toString());
		float sum = 0;
		float singleSample;
		while(it.hasNext()) {
			singleSample = Float.parseFloat(it.next().toString());
			sum += Math.pow(singleSample - singleMean, 2);
		}
		float variance = sum / 2088; 
		context.write(new Text(key.getFirstKey()), new FloatWritable(variance));
		/*System.out.println("reduce : TextPaire.key " + key.toString() + ", tradeId = " + tradeId);*/
	}
}
