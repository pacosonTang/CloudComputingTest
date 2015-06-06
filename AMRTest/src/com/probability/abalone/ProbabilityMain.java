package com.probability.abalone;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient.Conf;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.abalone.MeanAbalone;
import com.testOne.FirstInputFormat;
import com.testOne.FirstMap;
import com.testOne.SecondInputFormat;
import com.testOne.SecondMap;
/**
 * 求各样本条件概率
 * 
 * @author Tang Rong (Pacoson Tang)
 * @three input files： 测试集abaline-Test.txt; 
 *  				    均值方差文件 /meanVarianceAbaone/part-r-00000（cache）
 *   their data structure are in the last of this file.
 * @output 各个样本属于某类到高斯概率。  
 */
public class ProbabilityMain {

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		
		String output = "hdfs://hadoop5:9000/test/probabilityAbalone/";
		// 检查输出文件是否存在, 存在即删除
		MeanAbalone.checkFileExisting(conf, output);
		
		//添加hdfs的文件meanVarianceAbalone到分布式缓存
		DistributedCache.addCacheFile(new URI("hdfs://192.168.119.105:9000/test/outMeanVarianceAbalone/part-r-00000#meanVarianceAbalone"), conf);
			
		//添加hdfs的文件meanAbalone到分布式缓存
		DistributedCache.addCacheFile(new URI("hdfs://192.168.119.105:9000/test/outMeanAbalone/part-r-00000#meanAbalone"), conf);
		
		Job job = new Job(conf, "sort");
		job.setJarByClass(ProbabilityMain.class);
		
		job.setMapperClass(ProbabilityMapper.class);
		job.setReducerClass(ProbabilityReduce.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		/*// map完了之后， 再sorting
		job.setSortComparatorClass(SortComparator.class);

		// 每个reducer，先reduce后grouping
		job.setGroupingComparatorClass(GroupComparator.class);*/

		job.setPartitionerClass(KeyPartition.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		
		FileInputFormat.addInputPath(job, new Path("hdfs://hadoop5:9000/test/inTestAbalone"));
		FileOutputFormat.setOutputPath(job, new Path(output));
        
		job.waitForCompletion(true);
	}
	
	/**
	 *  data structure (Cache)
	 *  
	 *  meanVarianceAbalone.txt (当数据是连续型时，它才有样本方差, 进而求出高斯分布概率; 
	 *  					离散型数据有概率即可, 当数据属性为infant,male,female,rings, 它们是离散型的)
	 *  
	 *  	类别（age）@属性 mean,variance
		    9@whole_weight	0.234,0.08014381
			9@viscera_weight	0.134,0.0015851812
			9@shucked_weight	0.469,0.0068059317
			9@shell_weight	0.2095,0.0015192057
			9@length	0.56,0.0013286176
			9@height	0.125,1.6412028E-4
			9@diameter	0.265,0.0048182155
			8@whole_weight	0.7165,0.013627777
			8@viscera_weight	0.085,0.0010991353
			8@shucked_weight	0.4735,0.007563967
			8@shell_weight	0.228,0.0011952647
			8@length	0.475,0.0010818252
			8@height	0.12,5.85346E-4
			8@diameter	0.41,7.554714E-4
			
		abalone-Test.txt
sample structure:	Sex(M,F,I), Length,Diameter,Height,Whole weight,Shucked weight,Viscera weight,Shell weight,Rings
			M,0.35,0.265,0.09,0.2255,0.0995,0.0485,0.07,7(9)
			M,0.44,0.365,0.125,0.516,0.2155,0.114,0.155,10(9)
	 */ 
}
