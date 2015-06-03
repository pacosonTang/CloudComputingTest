package com.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient.Conf;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JoinMain {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "sort");
		job.setJarByClass(JoinMain.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);

		job.setMapperClass(JoinMapper.class);
		job.setReducerClass(JoinReduce.class);

		job.setMapOutputKeyClass(TextPaire.class);
		job.setMapOutputValueClass(Text.class);

		// map完了之后， 再sorting
		job.setSortComparatorClass(SortComparator.class);

		// 每个reducer，先reduce后grouping
		job.setGroupingComparatorClass(GroupComparator.class);

		job.setPartitionerClass(KeyPartition.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(
				"hdfs://hadoop5:9000/test/join/"));
		FileOutputFormat.setOutputPath(job, new Path(
				"hdfs://hadoop5:9000/test/outJoin2"));
		job.waitForCompletion(true);
	}
}
