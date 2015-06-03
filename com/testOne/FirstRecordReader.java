package com.testOne;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

/**
 * structure: 8个逗号 
 * @author hadoop
 *
 */
public class FirstRecordReader extends RecordReader<Text, FirstClass> {  
	  
    // 定义一个真正读取split中文件的读取器  
    private LineRecordReader lineRecordReader = null;  
    private Text key = null;  
    private FirstClass value = null;  
  
    public FirstRecordReader(){
    	super();
    	System.out.println("FirstRecordReader 的构造方法");
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
        // 没有读取到东西  
        if (!lineRecordReader.nextKeyValue()) {  
            key = null;  
            value = null;  
            return false;  
        }  
        Text val = lineRecordReader.getCurrentValue();  
        String line = val.toString();  
        String[] str = line.split("t");  
        key = new Text(str[0]);  
        value = new FirstClass(str[1].trim()); // 实现对原始数据的预分割  
        return true;  
    }  
  
    // 读取key的当前值  
    @Override  
    public Text getCurrentKey() throws IOException, InterruptedException {  
        return key;  
    }  
  
    // 读取value的当前值  
    @Override  
    public FirstClass getCurrentValue() throws IOException,  
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