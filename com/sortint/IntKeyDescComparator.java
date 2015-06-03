package com.sortint;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.A;

/**
 * int的key按照降序排列
 * 
 * @author zhangdonghao
 * 
 */
public class IntKeyDescComparator extends WritableComparator {

	protected IntKeyDescComparator() {
		super(IntWritable.class, true);
		System.out.println("这是 IntKeyDescComparator 构造方法");
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		System.out.println("IntKeyDescComparator.compare 方法" + ", a = " + a.toString() + ", b = " + b.toString());
		return -super.compare(a, b);//取负号是为了降序
	}

}