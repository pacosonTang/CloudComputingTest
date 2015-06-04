package com.variance.abalone;

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
		super(TextPaire.class,true); //注册comparator
	}

	@Override
	@SuppressWarnings("all")
	public int compare(WritableComparable a, WritableComparable b) {
		TextPaire a1 = (TextPaire)a;
		TextPaire b1 = (TextPaire)b;
		System.out.println("Grouping: GroupingComparator.compare()方法" + "a = " + a1.getFirstKey() + ", b = " + b1.getFirstKey());
		return -a1.getFirstKey().compareTo(b1.getFirstKey());
	}
}
