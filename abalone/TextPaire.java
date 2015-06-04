package com.variance.abalone;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * 自定义类型
 * 
 * @author Tang Rong (Pacoson Tang)
 *
 */

public class TextPaire implements WritableComparable<TextPaire> {

	private String firstKey;
	private String secondKey;

	public TextPaire(){
		super();
	}
	
	public TextPaire(String firstKey, String secondKey) {
		super();
		this.firstKey = firstKey;
		this.secondKey = secondKey;
	}
	
	@Override
	public int compareTo(TextPaire o) { // 1 0 -1

		System.out.println("TextPaire.compareTo()方法");
		return o.getFirstKey().compareTo(this.getFirstKey());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.firstKey = in.readUTF();
		this.secondKey = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(this.firstKey);
		out.writeUTF(this.secondKey);
	}

	/*
	 * set and get methods
	 */
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return "firstKey = " + firstKey + ", secondKey = " + secondKey;
	}

	public String getFirstKey() {
		return firstKey;
	}

	public void setFirstKey(String firstKey) {
		this.firstKey = firstKey;
	}

	public String getSecondKey() {
		return secondKey;
	}

	public void setSecondKey(String secondKey) {
		this.secondKey = secondKey;
	}

}
