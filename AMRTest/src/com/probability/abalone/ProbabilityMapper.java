package com.probability.abalone;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 排序mapper
 * 
 * @author Tang Rong (Pacoson Tang)
 * 
 */
public class ProbabilityMapper extends Mapper<Object, Text, Text, IntWritable>{
	
	 float[][] meanArr = new float[31][7];
	 float[][] varianceArr = new float[31][7];
	 float[][] discreteArr = new float[31][4];//female,infant,male,rings

	 
	 /**
	  * setup方法 将缓存中到文件数据读取并进行结构化
	  */
	 @Override 
     protected void setup(Context context)   
             throws IOException, InterruptedException {   
		 
		 Configuration conf = new Configuration();
		 FileSystem fs = FileSystem.get(conf);
		 
		/* 测试专用
		 * Path path = new Path("meanVarianceAbalone");
		 FSDataInputStream fsdis = fs.open(path);
		 BufferedReader br = new BufferedReader(new InputStreamReader(fsdis));
		 
		 path = new Path("meanAbalone");
		 fsdis = fs.open(path);
		 BufferedReader br2 = new BufferedReader(new InputStreamReader(fsdis));*/
		 
		 
		 //获得当前作业的DistributedCache文件(meanVarianceAbalone) series   
		 BufferedReader br = new BufferedReader(new FileReader("meanVarianceAbalone"));
		 //获得当前作业的DistributedCache文件(meanAbalone) discrete
		 BufferedReader br2 = new BufferedReader(new FileReader("meanAbalone"));
		 
		 
		 //分别对连续型和离散型数据进行结构化
		 this.proc_series(br);
		 this.proc_discrete(br2);
     }

	@Override
	protected void map(Object key, Text value,
			Context context) 
			throws IOException, InterruptedException {
		/**
		 * data structure of abalone-test.txt
		 * structure:	Sex(M,F,I), Length,Diameter,Height,Whole weight,Shucked weight,Viscera weight,Shell weight,Rings
		 *  M,0.455,0.365,0.095,0.514,0.2245,0.101,0.15,15
			F,0.53,0.42,0.135,0.677,0.2565,0.1415,0.21,9
		 */
		System.out.println("map method starting"); 
		String[] line = value.toString().split(",");
		//line[1]=length,line[2]=diameter,line[3]=height,line[4]=whole_weight,
		//line[5]=shucked_weight,line[6]=viscera_weight,line[7]=shell_weight;,line[8]=rings;
		float[] priorities = {Float.parseFloat(line[4]),Float.parseFloat(line[6]),
							  Float.parseFloat(line[5]),Float.parseFloat(line[7]),
							  Float.parseFloat(line[1]),Float.parseFloat(line[3]),
							  Float.parseFloat(line[2])}; 
		float mean, variance, sample = 0;
		float con_probability = 1;
		float max_con_probability = 0;
		int estimate_rings = 0;
		for(int i = 1; i < 31; i++) {
			
			for(int j = 0; j < 7; j++) {//内层循环计算 sample的7个属性属于第i个类的条件概率 con_probability。
				mean = meanArr[i][j];
				variance = varianceArr[i][j];
				sample = priorities[j];
				con_probability *= gussi(mean,variance,sample);
			}
			for(int j = 0; j < 4; j ++) //条件概率 和 离散型属性分布概率 进行累积
				con_probability *= discreteArr[i][j];
			
			if ( con_probability > max_con_probability ) {
				con_probability = max_con_probability;
				estimate_rings = i; //把最大概率到 rings 保存起来
			}
			con_probability = 1;
		}
		 //将估计类同真实类进行比较，并写入 1(true) or 0（false）
		if(estimate_rings == Integer.parseInt(line[8]))
			context.write(new Text("1"), new IntWritable(1));
		else 
			context.write(new Text("0"), new IntWritable(0));
	}
	
	/** 构建计算 高斯分布 概率的函数
	 * 
	 * @param mean 均值
	 * @param variance 方差
	 * @param sample 样本值
	 * @return 条件概率
	 */
	public float gussi(float mean, float variance, float sample){
		
		float probability = 0; //条件概率 
		float power = 0; //欧拉数exp的幂
		
		power = (float) ( Math.pow(sample - mean, 2) / ( 2 * variance ) );
		probability = (float) ( 1.0 / ( Math.pow( 2 * Math.PI, 0.5) * variance )  * Math.exp(-power) ); 
		
		return probability;
	}
	
	/**对连续型数据进行处理 
	 * 
	 * @param br 输入文件字符流 
	 * @throws IOException
	 */
	public void proc_series(BufferedReader br) throws IOException{

		//1. 对meanVarianceAbalone 进行处理
		 /** data structure of meanVarianceAbalone
		  * 9@whole_weight	0.234,0.08014381
			9@viscera_weight	0.134,0.0015851812
			9@shucked_weight	0.469,0.0068059317
			9@shell_weight	0.2095,0.0015192057
			9@length	0.56,0.0013286176
			9@height	0.125,1.6412028E-4
			9@diameter	0.265,0.0048182155
		  */
		 int col = 0;
		 String value = null;
		 while((value = br.readLine()) != null) {
	        String[] line = value.toString().split(",");//line[1]=0.0801 （variance）
	        String[] subline = line[0].split("\t");//subline[1]=0.234 （mean）
	        System.out.println("line[1]" + line[1]);
	        int key = Integer.parseInt(subline[0].split("@")[0]);
	        meanArr[key][col] = Float.parseFloat(line[1]);
	        varianceArr[key][col++] = Float.parseFloat(subline[1]);
	        if(col % 7 == 0)
	        	col = 0;//最后要把列号赋值为0
        }
	}
	
	/**对离散型数据进行处理 
	 * 
	 * @param br 输入文件字符流 
	 * @throws IOException
	 */ 
	public void proc_discrete(BufferedReader br) throws IOException{
		
		//2. 对meanAbalone进行处理
		 /**
		  * 10@diameter	,0.0697749
			10@female	,0.055555556
			10@height	,0.023924796
			10@infant	,0.024425287
			10@length	,0.08930559
			10@male	,0.07614943
			10@rings	,0.15613027
			10@shell_weight	,0.04368967
			10@shucked_weight	,0.06909029
			10@viscera_weight	,0.03415421
			10@whole_weight	,0.15542044
		  */
		 String value = null;
		 int key;
		 while((value = br.readLine()) != null) {
			if( !(value.contains("infant") || value.contains("male") || value.contains("female") || value.contains("rings")) )
				continue;
	        String[] line = value.toString().split(",");//line[1]=0.055555556 
	        String[] subline = line[0].trim().split("@");//subline[0]=10, subline[1]=female
	        System.out.println("line[1]" + line[1]);
	        key = Integer.parseInt(subline[0]);
	        if(subline[1].equals("female"))
	        	discreteArr[key][0] = Float.parseFloat(line[1]);
	        else if(subline[1].equals("infant"))
	        	discreteArr[key][1] = Float.parseFloat(line[1]);
	        else if(subline[1].equals("male"))
	        	discreteArr[key][2] = Float.parseFloat(line[1]);
	        else//rings
	        	discreteArr[key][3] = Float.parseFloat(line[1]);
		 }
	}
}
