package com.variance.abalone;

import java.io.IOException;

import javax.sound.sampled.Line;

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
public class SecondRecordReader extends RecordReader<Text, SecondClass> {  
	  
    // 定义一个真正读取split中文件的读取器  
    private LineRecordReader lineRecordReader = null;  
    private Text key = null;  
    private SecondClass value = null;  
  
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
        //data structure: 1	1.121705E-6,2.8042626E-7,5.608525E-7,8.4127873E-7,5.608525E-4,0.0,4.2063937E-5,5.608525E-4,5.608525E-6,0.0,3.0846888E-5,
        String[] subarr = line.split("\t");
        key = new Text(subarr[0]);
        value = new SecondClass("means@" + subarr[1].substring(0, subarr.length-1)); // 实现对原始数据的预分割, substring(begin,end) 包括begin，不包括end  
        return true;  
    }  
  
    // 读取key的当前值  
    @Override  
    public Text getCurrentKey() throws IOException, InterruptedException {  
        return key;  
    }  
  
    // 读取value的当前值  
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