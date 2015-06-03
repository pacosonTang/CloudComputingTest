package com.sortint;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Main {

	public static void main(String[] args) throws Exception {
		/**
		 * 这里是map输出的key和value类型
		 */
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(SortIntValueMapper.class);
		// job.setCombinerClass(WordCountReduce.class);
		job.setReducerClass(SortIntValueReduce.class);
		
		// key按照降序排列
		job.setSortComparatorClass(IntKeyDescComparator.class);
		//分区策略 
		job.setPartitionerClass(KeySectionPartitioner.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		/**
		 * 这里可以放输入目录数组，也就是可以把上一个job所有的结果都放进去
		 **/
		FileInputFormat.setInputPaths(job, new Path("/test/inSort/data.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/test/outSort5/"));
		job.waitForCompletion(true);
	}
}
