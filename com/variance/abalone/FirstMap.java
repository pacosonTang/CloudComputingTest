package com.variance.abalone;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FirstMap extends Mapper<Text, FirstClass, Text, Text> {  
    @Override  
    protected void map(Text key, FirstClass value,  
            Mapper<Text, FirstClass, Text, Text>.Context context)  
            throws IOException, InterruptedException {  
        context.write(key, new Text(value.toString()));  
    }  
}  