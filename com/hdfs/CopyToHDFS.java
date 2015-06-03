package com.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CopyToHDFS {
	
	public static void main(String[] args) throws IOException {
		
		//step1: 获取配置信息
		Configuration conf = new Configuration();
		//step2: 获取filesystem 
		FileSystem fs = FileSystem.get(conf);
		Path source = new Path("../../hdfs-input/abalone/abalone-Train.txt");            
		Path des = new Path("/test/variAbalone");
		//step3: 拷贝文件到
		fs.mkdirs(new Path("/test/in/1506011120"));
		
	}
}
