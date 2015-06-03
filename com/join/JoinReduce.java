package com.join;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReduce extends Reducer<TextPaire, IntWritable, Text, Text>{

	@Override
	protected void reduce(TextPaire key, Iterable<IntWritable> values,
			Context context)
			throws IOException, InterruptedException {
		
		String tradeId = values.iterator().next().toString();//first value is tradeID
		while(values.iterator().hasNext()) {
			String payId = values.iterator().next().toString();
			context.write(new Text(tradeId), new Text(payId));
		}
	}
}
