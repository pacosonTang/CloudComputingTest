package com.variance.abalone;

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
        int pos = line.lastIndexOf(",");//找出最后一个逗号到pos
        key = new Text(line.substring(pos+1));
        value = new FirstClass(line.substring(2, pos)); // 实现对原始数据的预分割, substring(begin,end) 包括begin，不包括end  
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