package com.abalone;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class Abalone {
	
	private String outputDir;
	private String inputpath;
	private String outputTrain;
	private String outputTest;
	private BufferedReader br;
	private BufferedWriter bwtrain;
	private BufferedWriter bwtest;
	
	public Abalone() throws IOException{
		this.outputDir = "/home/hadoop/workspace/MRTest/src/hdfs-input/abalone";
		this.inputpath = this.outputDir + "/abalone-data.txt";
		this.outputTrain = this.outputDir + "/abalone-Train.txt";
		this.outputTest = this.outputDir + "/abalone-Test.txt";
		this.br = new BufferedReader(new FileReader(this.inputpath));
		this.bwtrain = new BufferedWriter(new FileWriter(this.outputTrain));
		this.bwtest = new BufferedWriter(new FileWriter(this.outputTest));
	}
	
	public static void main(String[] args) throws IOException {
		
		Abalone abalone = new Abalone();
		abalone.splitHalf();
		System.out.println("划分数据完成 ");
		
		System.out.println("性别数据映射完成");
	}
	public void mapsex(String str) {
		
	}
	public void splitHalf() throws IOException{
		
		String str = null;
		while((str = this.br.readLine()) != null){
			
			bwtrain.write(str);
			bwtrain.newLine();
			if((str = this.br.readLine()) != null) {
				bwtest.write(str);
				bwtest.newLine();
			}
			else
				break;
		}
	}
}









