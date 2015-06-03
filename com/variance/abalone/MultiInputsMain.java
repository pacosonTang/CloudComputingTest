package com.variance.abalone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MultiInputsMain extends Configuration implements Tool {  
    private String input1 = null; // 定义的多个输入文件  
    private String input2 = null;  
    private String output = null;  
  
    @Override  
    public void setConf(Configuration conf) {  
    
    }  
  
    @Override  
    public Configuration getConf() {  
        return new Configuration();  
    }  
  
    @Override  
    public int run(String[] args) throws Exception {  
        //setArgs(args);  
       // checkParam();// 对参数进行检测  
  
        Configuration conf = new Configuration();  
        Job job = new Job(conf);  
        job.setJarByClass(MultiInputsMain.class);  
  
        job.setMapOutputKeyClass(Text.class);  
        job.setMapOutputValueClass(Text.class);  
  
        job.setReducerClass(MultiInputsRedu.class);  
        job.setOutputKeyClass(Text.class);  
        job.setOutputValueClass(Text.class);  
  
        // MultipleInputs类添加文件路径  
        // 添加上自定义的fileInputFormat(分别是FirstInputFormat和SecondInputFormat)格式  
        MultipleInputs.addInputPath(job, new Path("/test/in/testOne/a.txt"),  
                FirstInputFormat.class, FirstMap.class);  
        MultipleInputs.addInputPath(job, new Path("/test/in/testOne/b.txt"),  
                SecondInputFormat.class, SecondMap.class);  
  
        FileOutputFormat.setOutputPath(job, new Path("/test/outOne"));  
        job.waitForCompletion(true);  
        return 0;  
    }  
  
    private void checkParam() {  
        if (input1 == null || "".equals(input1.trim())) {  
            System.out.println("no input phone-data path");  
            userMaunel();  
            System.exit(-1);  
        }  
        if (input2 == null || "".equals(input2.trim())) {  
            System.out.println("no input user-data path");  
            userMaunel();  
            System.exit(-1);  
        }  
        if (output == null || "".equals(output.trim())) {  
            System.out.println("no output path");  
            userMaunel();  
            System.exit(-1);  
        }  
    }  
  
    // 用户手册  
    private void userMaunel() {  
        System.err.println("Usage:");  
        System.err.println("-i1 input \t phone data path.");  
        System.err.println("-i2 input \t user data path.");  
        System.err.println("-o output \t output data path.");  
    }  
  
    // 对属性进行赋值  
    // 设置输入的格式:-i1 xxx(输入目录) -i2 xxx(输入目录) -o xxx(输出目录)   
    private void setArgs(String[] args) {  
        for (int i = 0; i < args.length; i++) {  
            if ("-i1".equals(args[i])) {  
                input1 = args[++i]; // 将input1赋值为第一个文件的输入路径  
            } else if ("-i2".equals(args[i])) {  
                input2 = args[++i];  
            } else if ("-o".equals(args[i])) {  
                output = args[++i];  
            }  
        }  
    }  
  
    public static void main(String[] args) throws Exception {  
        Configuration conf = new Configuration();  
        ToolRunner.run(conf, new MultiInputsMain(), args); // 调用run方法  
    }  
} 