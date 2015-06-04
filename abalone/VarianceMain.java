package com.variance.abalone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient.Conf;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.testOne.FirstInputFormat;
import com.testOne.FirstMap;
import com.testOne.SecondInputFormat;
import com.testOne.SecondMap;
/**
 * 自定义类型
 * 
 * @author Tang Rong (Pacoson Tang)
 *
 */
public class VarianceMain {

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = new Job(conf, "sort");
		job.setJarByClass(VarianceMain.class);
		
		job.setMapperClass(VarianceMapper.class);
		job.setReducerClass(VarianceReduce.class);

		job.setMapOutputKeyClass(TextPaire.class);
		job.setMapOutputValueClass(Text.class);

		// map完了之后， 再sorting
		job.setSortComparatorClass(SortComparator.class);

		// 每个reducer，先reduce后grouping
		job.setGroupingComparatorClass(GroupComparator.class);

		job.setPartitionerClass(KeyPartition.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		
		FileInputFormat.addInputPath(job, new Path("hdfs://hadoop5:9000/test/meanAbalone/part-r-00000"));
		FileInputFormat.addInputPath(job, new Path("hdfs://hadoop5:9000/test/abalone/abalone-Train.txt"));
		
		FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop5:9000/test/varianceAbalone/"));
        
		job.waitForCompletion(true);
	}
	
	/**
	 *  sample.txt
	 *       M,0.455,0.365,0.095,0.514,0.2245,0.101,0.15,15
		     F,0.53,0.42,0.135,0.677,0.2565,0.1415,0.21,9
structure:	Sex(M,F,I), Length,Diameter,Height,Whole weight,Shucked weight,Viscera weight,Shell weight,Rings	

		means.txt
		    10@diameter	,0.062296707
			10@female	,0.05776781
			10@height	,0.021332027
			10@infant	,0.018508133
			10@length	,0.07961585
			10@male	,0.06281548
			10@rings	,0.13909142
			10@shell_weight	,0.038752668l
			10@shucked_weight	,0.061760522
			10@viscera_weight	,0.031088633
			10@whole_weight	,0.13918906
			
			output.txt (sample.txt - means.txt)
			
			对于means.txt 每行数据：  <'key 1',value> <10@diameter	1, 0.062296707>
			对于sample.txt 每行数据： <'key 2',value> <10@diameter	2, 0.455>
	 */ 
	 
}
