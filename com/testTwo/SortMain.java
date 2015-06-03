package com.testTwo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SortMain {

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", " ");
		
		Job job = new Job(conf, "sort");
		job.setJarByClass(SortMain.class);
		
		//设置输入格式
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		//设置 map 和 reduce 方法所在的函数
		job.setMapperClass(SortMapper.class);
		job.setReducerClass(SortReduce.class);
		
		//设置map到输出键值格式
		job.setMapOutputKeyClass(IntPaire.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//设置比较类
		job.setSortComparatorClass(TextIntComparator.class);
		
		//设置聚集类（利用key来进行聚集，因为聚集是以key是否相等为前提的） 
		job.setGroupingComparatorClass(TextComparator.class);
		
		//设置 reduce 的个数
		job.setPartitionerClass(PartitionByText.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(
				"hdfs://hadoop5:9000/test/inSort/sort.txt"));
		FileOutputFormat.setOutputPath(job, new Path(
				"hdfs://hadoop5:9000/test/outSortTwo2"));

		job.waitForCompletion(true);
	}
}
