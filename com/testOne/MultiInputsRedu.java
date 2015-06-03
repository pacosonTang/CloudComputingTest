package com.testOne;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.inject.Key;

public class MultiInputsRedu extends Reducer<Text, Text, Text, Text> {  
	
	public MultiInputsRedu(){
		super();
		System.out.println("MultiInputsRedu构造方法");
	}
    @Override  
    protected void reduce(Text key, Iterable<Text> values,  
            Reducer<Text, Text, Text, Text>.Context context)  
            throws IOException, InterruptedException {  
        for (Text val : values) {  
            context.write(key, val);
            System.out.println("key = " + key.toString() + ", value = " + val.toString());
        }  
        System.out.println("一层 循环结束");
    }  
}