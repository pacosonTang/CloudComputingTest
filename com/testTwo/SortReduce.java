package com.testTwo;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SortReduce extends Reducer<IntPaire, IntWritable, Text, Text>{

	@Override
	protected void reduce(IntPaire key, Iterable<IntWritable> values,
			Context context)
			throws IOException, InterruptedException {
		
		StringBuffer combineValue = new StringBuffer();
		StringBuffer tempsb = null;
		Iterator<IntWritable> itr = values.iterator();
		while(itr.hasNext()){
			int value = itr.next().get();
			combineValue.append(value + ",");//字符串拼接
		}
		if(combineValue.length() > 0) //因为最后一个字符是逗号，截去它
			tempsb = new StringBuffer(combineValue.substring(0,combineValue.length()));
		context.write(new Text(key.getFirstKey()), new Text(tempsb.toString()));
	}
	
	
}
