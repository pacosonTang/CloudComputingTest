package com.testOne;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class FirstInputFormat extends FileInputFormat<Text, FirstClass> {  
	  
	public FirstInputFormat(){
		super();
		System.out.println("FirstInputFormat构造方法");
	}
	
    @Override  
    public RecordReader<Text, FirstClass> createRecordReader(InputSplit split,  
            TaskAttemptContext context) throws IOException,  
            InterruptedException {  
    	System.out.println("first: RecordReader方法");
        return new FirstRecordReader();  
    }  
}  