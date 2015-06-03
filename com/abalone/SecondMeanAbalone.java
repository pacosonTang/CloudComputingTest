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

import sun.beans.editors.IntEditor;
/**
 * 对数据结构 10@diameter	0.06229671 进行foramt
 * @author hadoop
 *结果处理后的数据到顺序是（从左到右）
 *1@whole_weight	1.121705E-6
 *1@viscera_weight	2.8042626E-7
 *1@shucked_weight	5.608525E-7
 *1@shell_weight	8.4127873E-7 
 *1@rings	5.608525E-4
 *1@male	0.0
 *1@length	4.2063937E-5
 *1@infant	5.608525E-4
 *1@height	5.608525E-6
 *1@female	0.0
 *1@diameter	3.0846888E-5
 */
public class SecondMeanAbalone {

	public static class Map extends
			Mapper<LongWritable, Text, Text, FloatWritable> {
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			//structure: sex,length,diameter,height,whole-weight,shucked-weight,viscera-weight,shell-weight,rings
			// 对数据结构 10@diameter	0.06229671 进行foramt 
			//每行按照 @ 进行划分
			String[] arr = value.toString().split("@");
			//类别age
			Text age = new Text(arr[0]);
			String[] subarr = arr[1].split("\t");
			System.out.println("subarr[0] = " + subarr[0] + "subarr[1] = " + subarr[1]);
			//将key and value 写入文件
			context.write(age, new FloatWritable(Float.parseFloat(subarr[1])));
		}
	}

	public static class Reduce extends
			Reducer<Text, FloatWritable, Text, Text> {

		public void reduce(Text key, Iterable<FloatWritable> values,
				Context context) throws IOException, InterruptedException {
			Iterator<FloatWritable> iterator = values.iterator();
			String meanstr = "";
			while(iterator.hasNext()) 
				meanstr += String.valueOf(iterator.next().get()) + ",";
			Text text = new Text(meanstr);
			context.write(key, text);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "wordcount");
		
		job.setJarByClass(SecondMeanAbalone.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(
				"hdfs://hadoop5:9000/test/meanAbalone/part-r-00000"));
		FileOutputFormat.setOutputPath(job, new Path(
				"hdfs://hadoop5:9000/test/secondMeanAbalone"));
		job.waitForCompletion(true);
	}
}
