package com.join;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class JoinMain {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "sort");
		job.setJarByClass(JoinMain.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setMapperClass(JoinMapper.class);
		job.setReducerClass(JoinReduce.class);

		job.setMapOutputKeyClass(TextPaire.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setSortComparatorClass(FirstComparator.class);
		
		job.setGroupingComparatorClass(FirstComparator.class);
		
		job.setPartitionerClass(KeyPartition.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(
				"hdfs://hadoop5:9000/test/inSort/sort.txt"));
		FileOutputFormat.setOutputPath(job, new Path(
				"hdfs://hadoop5:9000/test/outSortTwo"));

		job.waitForCompletion(true);
	}
}
