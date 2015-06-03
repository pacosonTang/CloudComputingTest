package com.abalone;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSOperation {
	
	private FileSystem fs;
	public HDFSOperation() throws IOException{
		//step1: 获取配置信息
		Configuration conf = new Configuration();
		//step2: 获取filesystem 
		FileSystem fs = FileSystem.get(conf);
	}
	public void copyFileToHDFS(String s, String d) throws IOException{
		
		Path src = new Path(s);            
		Path dst = new Path(d);
     	//step3: 拷贝文件到
		this.fs.copyFromLocalFile(src, dst);
		System.out.println("copy suc");
	}
	
	public void deleteFromHDFS(String s) throws IOException{

		Path src = new Path(s);            
		this.fs.deleteOnExit(src);
		System.out.println("delete suc");
	}
}
