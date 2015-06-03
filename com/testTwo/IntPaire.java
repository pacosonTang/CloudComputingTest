package com.testTwo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import org.apache.hadoop.io.WritableComparable;

/**
 * 自定义类型
 * 
 * @author Tang Rong (Pacoson Tang)
 *
 */

public class IntPaire implements WritableComparable<IntPaire>{
 
	private String firstKey;
	private int secondKey;
	
	@Override
	public int compareTo(IntPaire o) {
		// TODO Auto-generated method stub
		return o.getFirstKey().compareTo(this.getFirstKey()); //1 0 -1
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.firstKey = in.readUTF();
		this.secondKey = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(firstKey);
		out.writeInt(secondKey);
	}
	
	/*
	 *set and get methods 
	 */
	public String getFirstKey() {
		return firstKey;
	}

	public void setFirstKey(String firstKey) {
		this.firstKey = firstKey;
	}

	public int getSecondKey() {
		return secondKey;
	}

	public void setSecondKey(int secondKey) {
		this.secondKey = secondKey;
	}
}
