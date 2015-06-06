package com.probability.abalone;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
/**
 * 自定义类型
 * 
 * @author Tang Rong (Pacoson Tang)
 *
 */
public class GroupComparator extends WritableComparator{

	public GroupComparator() {
		super(Text.class,true); //注册comparator
	}

	@Override
	@SuppressWarnings("all")
	public int compare(WritableComparable a, WritableComparable b) {
		Text a1 = (Text)a;
		Text b1 = (Text)b;
		return a1.toString().compareTo(b1.toString());
	}
}
