package com.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSMkdir {
	
	public static void main(String[] args) throws IOException {
		
		//step1: 获取配置信息
		Configuration conf = new Configuration();
		//step2: 获取filesystem 
		FileSystem fs = FileSystem.get(conf);
		
		//step3: 创建一个文件
		fs.mkdirs(new Path("/test/in/1506011120"));
		
		//step 4 查看文件状态
		FileStatus status = fs.getFileStatus(new Path("/test/in/1506011120"));
		status.getPath();
	}
}
