package com.variance.abalone;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SecondMap extends Mapper<Text, SecondClass, Text, Text> {  
    @Override  
    protected void map(Text key, SecondClass value,  
            Mapper<Text, SecondClass, Text, Text>.Context context)  
            throws IOException, InterruptedException {  
        context.write(key, new Text(value.toString()));  
    }  
}  