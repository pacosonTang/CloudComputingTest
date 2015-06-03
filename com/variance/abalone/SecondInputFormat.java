package com.variance.abalone;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class SecondInputFormat extends FileInputFormat<Text, SecondClass> {  
	  
    @Override  
    public RecordReader<Text, SecondClass> createRecordReader(InputSplit split,  
            TaskAttemptContext context) throws IOException,  
            InterruptedException {  
        return new SecondRecordReader();  
    }  
}  