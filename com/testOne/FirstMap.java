package com.testOne;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.inject.Key;

public class FirstMap extends Mapper<Text, FirstClass, Text, Text> {  
	
	public FirstMap(){
		super();
		System.out.println("FirstMap构造方法");
	}
	
    @Override  
    protected void map(Text key, FirstClass value,  
            Mapper<Text, FirstClass, Text, Text>.Context context)  
            throws IOException, InterruptedException {  
    	System.out.println("first  key = " + key + "value = " + value);
        context.write(key, new Text(value.toString()));  
    }  
}  