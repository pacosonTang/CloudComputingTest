package com.variance.abalone;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MultiInputsRedu extends Reducer<Text, Text, Text, Text> {  
    @Override  
    protected void reduce(Text key, Iterable<Text> values,  
            Reducer<Text, Text, Text, Text>.Context context)  
            throws IOException, InterruptedException {  
    	Iterator<Text> iterator = values.iterator();
    	//values: means@1.121705E-6,2.8042626E-7,5.608525E-7,8.4127873E-7,5.608525E-4,0.0,4.2063937E-5,5.608525E-4,5.608525E-6,0.0,3.0846888E-5,
    	while(iterator.hasNext()) {
    		String str = iterator.next().toString();
    		//if(str.contains("means"))//说明它是均值value
    			
    	}
        for (Text val : values) {  
            context.write(key, val);
            System.out.println("key = " + key.toString() + ", value = " + val.toString());
        }  
        System.out.println("一层 循环结束");
    }  
    
    public void buildMeans(String str){
    	
    	String[] arr = str.split(","); 
    	AbaloneMeans am = new AbaloneMeans(str[], str, str, str, str, str, str, str, str, str, str);
    }
}