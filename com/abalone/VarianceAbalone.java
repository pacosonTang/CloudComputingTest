package com.abalone;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.commons.math3.stat.descriptive.summary.Sum;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class VarianceAbalone {

	public static class Map extends
			Mapper<LongWritable, Text, Text, FloatWritable> {
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			//每行按照逗号进行划分(每行8个逗号)
			String[] arr = value.toString().split(",");
			//structure: sex,length,diameter,height,whole-weight,shucked-weight,viscera-weight,shell-weight,rings
			//构造8个key
			Text sex = new Text(arr[8]+"@sex@vari"); //sex
			Text length = new Text(arr[8]+"@length@vari"); //length
			Text diameter = new Text(arr[8]+"@diameter@vari"); //diameter 
			Text height = new Text(arr[8]+"@height@vari"); //height
			Text whole_weight = new Text(arr[8]+"@whole_weight@vari"); //whole_weight 
			Text shucked_weight = new Text(arr[8]+"@shucked_weight@vari"); //shucked_weight 
			Text viscera_weight = new Text(arr[8]+"@viscera_weight@vari"); //viscera_weight 
			Text shell_weight = new Text(arr[8]+"@shell_weight@vari"); //shell_weight
			Text rings = new Text(arr[8]+"@rings@vari"); //rings
			Text female = new Text(arr[8]+"@female@vari"); //female
			Text male = new Text(arr[8]+"@male@vari"); //male
			Text infant = new Text(arr[8]+"@infant@vari"); //infant
			
			//将key and value 写入文件
			
			context.write(length, new FloatWritable(Float.parseFloat(arr[1])));
			context.write(diameter, new FloatWritable(Float.parseFloat(arr[2])));
			context.write(height, new FloatWritable(Float.parseFloat(arr[3])));
			context.write(whole_weight, new FloatWritable(Float.parseFloat(arr[4])));
			context.write(shucked_weight, new FloatWritable(Float.parseFloat(arr[5])));
			context.write(viscera_weight, new FloatWritable(Float.parseFloat(arr[6])));
			context.write(shell_weight, new FloatWritable(Float.parseFloat(arr[7])));
			context.write(rings, new FloatWritable(1));
			if(arr[0].equals("F"))
				context.write(female, new FloatWritable(1));
			else if(arr[0].equals("M"))
				context.write(male, new FloatWritable(1));
			else
				context.write(infant,new FloatWritable(1));
		}
	}

	public static class Reduce extends
			Reducer<Text, FloatWritable, Text, FloatWritable> {

		public void reduce(Text key, Iterable<FloatWritable> values,
				Context context) throws IOException, InterruptedException {
			float sum = 0;
			Iterator<FloatWritable> iterator = values.iterator();
			while(iterator.hasNext()) 
				sum += iterator.next().get();
			int count = 1783;
			float aver = sum / count;
			context.write(key, new FloatWritable(aver));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "wordcount");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(
				"hdfs://hadoop5:9000/test/meanAbalone"));
		FileOutputFormat.setOutputPath(job, new Path(
				"hdfs://hadoop5:9000/test/varianceAbalone"));
		HDFSOperation ho = new HDFSOperation();
		//拷贝样本数据和均值数据处于统一个文件
		ho.copyFileToHDFS("/test/meanAbalone", "../../hdfs-input/abalone/abalone-Train.txt");
		job.waitForCompletion(true);
	}
}
