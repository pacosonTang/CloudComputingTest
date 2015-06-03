package com.testOne;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class SecondRecordReader extends RecordReader<Text, SecondClass> {  
    // 定义一个真正读取split中文件的读取器  
    private LineRecordReader lineRecordReader = null;  
    private Text key = null;  
    private SecondClass value = null;  
  
    public SecondRecordReader(){
    	super();
    	System.out.println("SecondRecordReader 的构造方法");
    }
    
    @Override  
    public void initialize(InputSplit split, TaskAttemptContext context)  
            throws IOException, InterruptedException {  
        close();  
        lineRecordReader = new LineRecordReader();  
        lineRecordReader.initialize(split, context);  
    }  
  
    @Override  
    public boolean nextKeyValue() throws IOException, InterruptedException {  
        if (!lineRecordReader.nextKeyValue()) {  
            key = null;  
            value = null;  
            return false;  
        }  
        Text val = lineRecordReader.getCurrentValue();  
        String line = val.toString();  
        String str[] = line.split("t");  
        key = new Text(str[0]);  
        value = new SecondClass(str[1], Integer.parseInt(str[2]));  
        return true;  
    }  
  
    @Override  
    public Text getCurrentKey() throws IOException, InterruptedException {  
        return key;  
    }  
  
    @Override  
    public SecondClass getCurrentValue() throws IOException,  
            InterruptedException {  
        return value;  
    }  
  
    @Override  
    public float getProgress() throws IOException, InterruptedException {  
        return lineRecordReader.getProgress();  
    }  
  
    @Override  
    public void close() throws IOException {  
        if (null != lineRecordReader) {  
            lineRecordReader.close();  
            lineRecordReader = null;  
        }  
        key = null;  
        value = null;  
    }  
  
}  